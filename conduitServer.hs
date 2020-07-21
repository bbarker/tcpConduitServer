#!/usr/bin/env stack
{- stack script --nix --resolver lts-14.27
  --nix-packages zlib
  --no-nix-pure
  --package bytestring
  --package classy-prelude
  --package conduit
  --package exceptions
  --package mtl
  --package network
  --package network-simple
  --package stm
  --package stm-conduit
  --package text
  --package unliftio
  --ghc-options -Wall
-}
{-# LANGUAGE NoImplicitPrelude #-}

{-# LANGUAGE RankNTypes                  #-}
{-# LANGUAGE ScopedTypeVariables         #-}

{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE PackageImports #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE AllowAmbiguousTypes #-}

module Main where

import           ClassyPrelude                    hiding (hClose)
import           Conduit
import           Control.Concurrent.STM.TBQueue   (TBQueue, writeTBQueue)
import           Control.Monad.Catch              (MonadMask)
import           Control.Monad.Loops
import           Control.Monad.Writer
import           Data.Bits                        (shiftR, (.&.))
import qualified Data.ByteString.Char8            as B
import           Data.Conduit.Async               (gatherFrom)
import qualified Data.Conduit.List                as CL
import           Data.Function                    ((&))
import qualified Data.Text                        as T
import           GHC.IO.Handle                    (Handle, hClose)
import qualified Network.Simple.TCP               as TCP
import qualified Network.Socket                   as NS
import qualified System.IO                        as IO
import           UnliftIO.Concurrent              (ThreadId, forkIO, threadDelay)

type Error = [String]
type Result r = Writer Error r

runResult :: Result r -> (r, Error)
runResult = runWriter

getPort :: NS.ServiceName
getPort = "29876"

waitForever :: IO ()
waitForever = do
  threadDelay 10000
  waitForever


-- | Wraps multiple calls to decodeMessageDelimitedH; instead of returning a list, we
-- | maintain the type signature for simplicity.
decodeMessages :: Handle -> IO (Either String String)
decodeMessages h = do
  putStrLn $ T.pack "starting decodeMessages"
  msgs <- go ""
  putStrLn $ T.pack "finishing decodeMessages"
  pure msgs
  where
    go :: String -> IO (Either String String)
    go xs = do
      isR <- IO.hReady h
      case isR of
        False -> do
          putStrLn $ T.pack "finishing go"
          pure $ Right xs
        True -> do
          sOut <- B.unpack <$> B.hGetLine h
          putStrLn $ T.pack $ "looping in go: " <> sOut <> xs
          go (sOut <> xs)
      
-- | This signature is meant to simulate the same function from the proto-lens library,
-- | but without dealing with protobus for binary data.
decodeMessageDelimitedH :: Handle -> IO (Either String String)
decodeMessageDelimitedH h = do
    sOut <- B.hGetLine h
    pure $ Right $ B.unpack sOut

protoServe :: forall m. (MonadMask m, MonadResource m, MonadUnliftIO m) =>
     (String -> Result [String])
  -> ConduitT () [String] m ()
protoServe fromProto = start .| mapMC logFilterRead
  .| CL.catMaybes .| mapMC msgToRecs
  where
    port = trace "getting protobuf port" getPort
    start = do
      let enQserver = serveTBQ (TCP.HostIPv4) port (decodeProto . fst)
      mygatherFrom 10000 enQserver
    decodeProto :: NS.Socket -> m (Either String String)
    decodeProto sock = bracket
      connHandleIO
      (liftIO . hClose)
      (liftIO . decodeMessages)
      where
        connHandleIO :: m Handle
        connHandleIO = liftIO $ sockToHandle sock
    logFilterRead :: Either String String -> m (Maybe String)
    logFilterRead pEi = case pEi of
      Right p -> pure $ Just p
      Left err -> trace err $ pure Nothing
    msgToRecs :: String -> m [String]
    msgToRecs p = case runResult $ fromProto p of
      (rs, rErr) -> do
        when (not $ null rErr) $ pure $ trace (intercalate " " rErr) ()
        pure $ trace "completed msgToRecs" rs

-- | The handle only needs a read-view of the socket.  Note that a TBQeueue is
-- | mutable but has STM's runtime safety checks in place.
sockToHandle :: NS.Socket -> IO Handle
sockToHandle sock = NS.socketToHandle sock ReadMode

-- | Based on serve and listen from Network.Simple.TCP
-- | Unlike `serve`, which never returns, `serveTBQ` immediately returns
-- | a `TBQueue` of results.
serveTBQ :: forall a m. (MonadMask m, MonadUnliftIO m)
  => TCP.HostPreference -- ^ Host to bind.
  -> NS.ServiceName -- ^ Server service port name or number to bind.
  -> ((NS.Socket, NS.SockAddr) -> m a)
  -- ^ Computation to run in a different thread once an incoming connection is
  -- accepted. Takes the connection socket and remote end address.
  -> TBQueue a -- ^ enqueue computation results to this queue
  -> m ()
  -- ^ Returns a FIFO (queue) of results from concurrent requests
serveTBQ hp port rFun tbq = do
    _ <- async $ withRunInIO $ \run -> TCP.serve hp port $ \(csock, addr) -> do
      run $ void $ do
        rVal <- trace "retrieved rVal in finally-restore" rFun (csock, addr)
        atomically $ writeTBQueue tbq rVal
    putStrLn $ T.pack "exiting serveTBQ"

retryForever :: forall m a. MonadUnliftIO m => m a -> m a
retryForever prog = catchAny prog progRetry
  where
    progRetry :: SomeException -> m a
    progRetry ex = do
      putStrLn $ pack $ show ex
      threadDelay 4000000
      retryForever prog

-- | Safer interface to sinkNull
sinkUnits :: MonadResource m => ConduitT () Void m ()
sinkUnits = sinkNull


-- | Gather output values asynchronously from an action in the base monad and
--   then yield them downstream.  This provides a means of working around the
--   restriction that 'ConduitM' cannot be an instance of 'MonadBaseControl'
--   in order to, for example, yield values from within a Haskell callback
--   function called from a C library.
mygatherFrom :: forall m o. (MonadIO m, MonadUnliftIO m)
           => Int                -- ^ Size of the queue to create
           -> (TBQueue o -> m ()) -- ^ Action that generates output values
           -> ConduitT () o m ()
mygatherFrom size scatter = do
    liftIO $ putStrLn $ T.pack "entering gatherFrom"
    chan   <- liftIO $ newTBQueueIO (fromIntegral size)
    worker <- lift $ async (scatter chan)
    res <- gather worker chan
    liftIO $ putStrLn $ T.pack "leaving gatherFrom"
    pure res
  where
    gather :: Async b -> TBQueue o -> ConduitT i o m b
    gather worker chan = do
        (xs :: [o], mres) <- liftIO $ atomically $ do
            xs <- whileM (not <$> isEmptyTBQueue chan) (readTBQueue chan)
            (xs,) <$> pollSTM worker
        -- instead of doing traverse_, we can do one yield at a time
        traverse_ yield xs
        case mres of
            Just (Left e)  -> liftIO $ throwIO (e :: SomeException)
            -- Just (Right r) -> return r
            _                -> gather worker chan

main :: IO ()
main = retryForever $ do
  putStrLn $ T.pack "starting tcp server"
  let myProtoServe = protoServe (pure . words)
  xs <- myProtoServe .| mapMC (putStrLn . T.pack . (":: " <> ) . intercalate "_") .| sinkUnits & runConduitRes
  -- this next line will never be reached now:
  putStrLn $ T.pack $ "tcp server exited with value: " <> (show xs) 
  waitForever
