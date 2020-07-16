{-# LANGUAGE NoImplicitPrelude #-}

{-# LANGUAGE RankNTypes                  #-}
{-# LANGUAGE ScopedTypeVariables         #-}

module Main where

import           ClassyPrelude                    hiding (hClose)
import           Conduit
import           Control.Concurrent.STM.TBQueue   (TBQueue, writeTBQueue)
import qualified Control.Exception.Safe           as Ex
import           Control.Monad.Catch              (MonadMask)
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
      gatherFrom 10000 enQserver
    decodeProto :: NS.Socket -> m (Either String String)
    decodeProto sock = bracket
      connHandleIO
      (liftIO . hClose)
      (liftIO . decodeMessageDelimitedH)
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
        when (not $ null rErr) $ pure $ trace (intercalate "\n" rErr) ()
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
    _ <- async $ (putStrLn $ T.pack "entering serveTBQ async") >>
      (withRunInIO $ \run -> do
        putStrLn $ T.pack "before TCP.serve"
        myserve hp port $ \(lsock, _) -> do
          putStrLn $ T.pack "before run"
          run $ (putStrLn $ T.pack "before acceptTBQ") >> acceptTBQ lsock rFun tbq >> (putStrLn $ T.pack "after acceptTBQ"))
    putStrLn $ T.pack "exiting serveTBQ"

-- | Based on acceptFork from Network.Simple.TCP.
acceptTBQ :: forall a m.
  MonadUnliftIO m
  => NS.Socket -- ^ Listening and bound socket.
  -> ((NS.Socket, NS.SockAddr) -> m a)
  -- ^ Computation to run in a different thread once an incoming connection is
  -- accepted. Takes the connection socket and remote end address.
  -> TBQueue a
  -> m ThreadId
acceptTBQ lsock rFun tbq = mask $ \restore -> do
  putStrLn $ T.pack "entered acceptTBQ computation"
  (csock, addr) <- trace ("running restore-accept on lsock: " <> (show lsock)) $ restore (liftIO $ NS.accept lsock)
  onException (forkIO $ finally
    (restore $ do
      rVal <- trace "retrieved rVal in finally-restore" rFun (csock, addr)
      atomically $ writeTBQueue tbq rVal)
    (TCP.closeSock csock))
    (TCP.closeSock csock)

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

main :: IO ()
main = retryForever $ do
  putStrLn $ T.pack "starting tcp server"
  let myProtoServe = protoServe (pure . words)
  myProtoServe .| mapMC (putStrLn . T.pack . intercalate "_") .| sinkUnits & runConduitRes
  putStrLn $ T.pack "tcp server exited"


-- | Start a TCP server that accepts incoming connections and handles them
-- concurrently in different threads.
--
-- Any acquired sockets are properly shut down and closed when done or in case
-- of exceptions. Exceptions from the threads handling the individual
-- connections won't cause 'serve' to die.
--
-- Note: This function performs 'listen', 'acceptFork', so don't perform
-- those manually.
myserve
  :: MonadIO m
  => TCP.HostPreference -- ^ Host to bind.
  -> NS.ServiceName -- ^ Server service port name or number to bind.
  -> ((NS.Socket, NS.SockAddr) -> IO ())
  -- ^ Computation to run in a different thread once an incoming connection is
  -- accepted. Takes the connection socket and remote end address.
  -> m a -- ^ This function never returns.
myserve hp port k = liftIO $ do
    putStrLn $ T.pack "before listen"
    mylisten hp port $ \(lsock, _) -> do
       putStrLn $ T.pack "before serve forever"
       forever $ Ex.catch
          (void (TCP.acceptFork lsock k))
          (\se -> IO.hPutStrLn IO.stderr (x ++ show (se :: Ex.SomeException)))
  where
    x :: String
    x = "Network.Simple.TCP.serve: Synchronous exception accepting connection: "


-- | Bind a TCP listening socket and use it.
--
-- The listening socket is closed when done or in case of exceptions.
--
-- If you prefer to acquire and close the socket yourself, then use 'bindSock'
-- and 'closeSock', as well as 'listenSock' function.
--
-- Note: The 'NS.NoDelay', 'NS.KeepAlive' and 'NS.ReuseAddr' options are set on
-- the socket. The maximum number of incoming queued connections is 2048.
mylisten
  :: (MonadIO m, Ex.MonadMask m)
  => TCP.HostPreference -- ^ Host to bind.
  -> NS.ServiceName -- ^ Server service port name or number to bind.
  -> ((NS.Socket, NS.SockAddr) -> m r)
  -- ^ Computation taking the listening socket and the address it's bound to.
  -> m r
mylisten hp port = Ex.bracket ((putStrLn $ T.pack "entering listen bracket") >>
  (do putStrLn $ T.pack $ "entered listen-bracket do-block"
      x@(bsock,_) <- TCP.bindSock hp port
      putStrLn $ T.pack $ "bound socket " <> (show bsock)
      TCP.listenSock bsock (max 2048 NS.maxListenQueue)
      pure x))
  (TCP.closeSock . fst)