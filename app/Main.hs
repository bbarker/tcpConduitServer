{-# LANGUAGE NoImplicitPrelude #-}

{-# LANGUAGE RankNTypes                  #-}
{-# LANGUAGE ScopedTypeVariables         #-}

{-# LANGUAGE LambdaCase         #-}

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
import qualified Control.Exception.Safe           as Ex
import           Control.Monad.Catch              (MonadMask)
import           Control.Monad.Loops
import           Control.Monad.Writer
import           Data.Bits                        (shiftR, (.&.))
import qualified Data.ByteString.Char8            as B
import           Data.Conduit.Async
import qualified Data.Conduit.List                as CL
import qualified Data.Foldable                    as DF
import           Data.Function                    ((&))
import qualified Data.List as List
import qualified Data.Text                        as T
import           GHC.IO.Handle                    (Handle, hClose)
import qualified Network.Simple.TCP               as TCP
import qualified Network.Socket                   as NS
import qualified System.IO                        as IO
import           UnliftIO.Concurrent              (ThreadId, forkIO, threadDelay)


-- takeEmptyMVar :: IO ()
-- takeEmptyMVar = do
--   eMVar :: MVar () <- newEmptyMVar
--   takeMVar eMVar

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
      mygatherFrom 10000 enQserver
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
    threadDelay 9000000 -- For DEBUG
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

waitForever :: IO ()
waitForever = do
  threadDelay 10000
  waitForever

-- | Safer interface to sinkNull
sinkUnits :: MonadResource m => ConduitT () Void m ()
sinkUnits = sinkNull

main :: IO ()
main = retryForever $ do
  putStrLn $ T.pack "starting tcp server"
  let myProtoServe = protoServe (pure . words)
  myProtoServe .| mapMC (putStrLn . T.pack . intercalate "_") .| sinkUnits & runConduitRes
  putStrLn $ T.pack "tcp server exited"
  waitForever

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
      x@(bsock,_) <- mybindSock hp port
      putStrLn $ T.pack $ "bound socket " <> (show bsock)
      TCP.listenSock bsock (max 2048 NS.maxListenQueue)
      pure x))
  (TCP.closeSock . fst)


-- | Obtain a 'NS.Socket' bound to the given host name and TCP service port.
--
-- The obtained 'NS.Socket' should be closed manually using 'closeSock' when
-- it's not needed anymore.
--
-- Prefer to use 'listen' if you will be listening on this socket and using it
-- within a limited scope, and would like it to be closed immediately after its
-- usage or in case of exceptions.
--
-- Note: The 'NS.NoDelay', 'NS.KeepAlive' and 'NS.ReuseAddr' options are set on
-- the socket.
mybindSock
  :: MonadIO m
  => TCP.HostPreference -- ^ Host to bind.
  -> NS.ServiceName -- ^ Server service port name or number to bind.
  -> m (NS.Socket, NS.SockAddr) -- ^ Bound socket and address.
mybindSock hp port = liftIO $ do
    addrs <- NS.getAddrInfo (Just hints) (hpHostName hp) (Just port)
    tryAddrs $ case hp of
       TCP.HostIPv4 -> prioritize isIPv4addr addrs
       TCP.HostIPv6 -> prioritize isIPv6addr addrs
       TCP.HostAny  -> prioritize isIPv6addr addrs
       _        -> addrs
  where
    hints :: NS.AddrInfo
    hints = NS.defaultHints
      { NS.addrFlags = [NS.AI_PASSIVE]
      , NS.addrSocketType = NS.Stream }
    tryAddrs :: [NS.AddrInfo] -> IO (NS.Socket, NS.SockAddr)
    tryAddrs = \case
      [] -> fail "Network.Simple.TCP.bindSock: No addresses available"
      [x] -> useAddr x
      (x:xs) -> Ex.catch (useAddr x) (\(_ :: IOError) -> tryAddrs xs)
    useAddr :: NS.AddrInfo -> IO (NS.Socket, NS.SockAddr)
    useAddr addr = Ex.bracketOnError (newSocket addr) TCP.closeSock $ \sock -> do
      let sockAddr = NS.addrAddress addr
      NS.setSocketOption sock NS.NoDelay 1
      NS.setSocketOption sock NS.ReuseAddr 1
      NS.setSocketOption sock NS.KeepAlive 1
      when (isIPv6addr addr) $ do
         NS.setSocketOption sock NS.IPv6Only (if hp == TCP.HostIPv6 then 1 else 0)
      NS.bind sock sockAddr
      pure (sock, sockAddr)


-- | Extract the 'NS.HostName' from a 'Host' preference, or 'Nothing' otherwise.
hpHostName:: TCP.HostPreference -> Maybe NS.HostName
hpHostName (TCP.Host s) = Just s
hpHostName _        = Nothing

-- | Move the elements that match the predicate closer to the head of the list.
-- Sorting is stable.
prioritize :: (a -> Bool) -> [a] -> [a]
prioritize p = uncurry (++) . List.partition p

isIPv4addr :: NS.AddrInfo -> Bool
isIPv4addr x = NS.addrFamily x == NS.AF_INET

isIPv6addr :: NS.AddrInfo -> Bool
isIPv6addr x = NS.addrFamily x == NS.AF_INET6

newSocket :: NS.AddrInfo -> IO NS.Socket
newSocket addr = NS.socket (NS.addrFamily addr)
                           (NS.addrSocketType addr)
                           (NS.addrProtocol addr)


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
        (xs, mres) <- liftIO $ atomically $ do
            xs <- whileM (not <$> isEmptyTBQueue chan) (readTBQueue chan)
            (xs,) <$> pollSTM worker
        traverse_ yield xs
        case mres of
            Just (Left e)  -> liftIO $ throwIO (e :: SomeException)
            Just (Right r) -> return r
            Nothing        -> gather worker chan