(ns nfs.tcp
  "A TCP listener that carries the NFS and MOUNT programs.

  `:clj` only, and that is a boundary rather than a gap — the same one
  `multiformats.core/cid-of-file` draws. Everything that decides anything
  is portable `.cljc`; this namespace owns a socket, a thread per
  connection, and nothing else. A different host can replace it without
  touching a byte of protocol.

  ## One listener, both programs

  `mount_nfs` takes `port=` and `mountport=` because MOUNT was historically
  a separate daemon. Pointing both at the same port is legal and is one
  fewer thing to configure; ONC RPC already multiplexes by program number.

  ## No portmapper

  Program 100000 is how a client would *discover* the ports. A client told
  the ports explicitly does not ask, so this does not implement it — and
  binding 111 needs root, which is the thing this whole design exists to
  avoid."
  (:require [oncrpc.core :as rpc]
            [oncrpc.record :as record]
            [nfs.mount :as mount]
            [nfs.v3 :as nfs])
  (:import [java.io InputStream OutputStream ByteArrayOutputStream]
           [java.net ServerSocket Socket InetAddress]))

(defn- read-available!
  "Append whatever has arrived to `buf`, returning false at end of stream."
  [^InputStream in ^ByteArrayOutputStream buf ^bytes chunk]
  (let [n (.read in chunk)]
    (if (neg? n)
      false
      (do (.write buf chunk 0 n) true))))

(defn- serve-connection!
  [^Socket socket programs {:keys [max-message] :or {max-message (* 4 1024 1024)}}]
  (with-open [socket socket
              in (.getInputStream socket)
              out (.getOutputStream socket)]
    (let [chunk (byte-array 65536)]
      (loop [buf (ByteArrayOutputStream.)]
        (let [bs (.toByteArray buf)]
          (if-let [{:keys [message end]} (record/parse bs {:max-message max-message})]
            (do
              (when-let [reply (rpc/dispatch programs message)]
                (let [framed (record/frame reply)]
                  (.write ^OutputStream out ^bytes framed)
                  (.flush ^OutputStream out)))
              ;; Keep the tail: a client pipelines, and the bytes after this
              ;; message are the next call, not noise to discard.
              (let [rest (ByteArrayOutputStream.)]
                (.write rest bs (int end) (int (- (alength bs) end)))
                (recur rest)))
            (if (read-available! in buf chunk)
              (recur buf)
              nil)))))))

(defn start!
  "Serve `fs` at `dir` until the returned map's `:stop!` is called.

  Returns `{:port n :stop! f}`. Port 0 asks the OS for a free one and the
  answer is in `:port` — which is what a test wants and what a desktop app
  wants too, since a fixed port is a collision waiting for the second
  install."
  [{:keys [fs dir port bind boot-id backlog]
    :or {dir "/" port 0 bind "127.0.0.1" backlog 32}}]
  (let [programs (merge (nfs/program-map fs {:boot-id (or boot-id
                                                          (System/currentTimeMillis))})
                        (mount/program-map [{:dir dir :handle (nfs/-root fs)}]))
        server (ServerSocket. port backlog (InetAddress/getByName bind))
        running (atom true)
        accept (fn []
                 (while @running
                   (try
                     (let [s (.accept server)]
                       (doto (Thread.
                              #(try (serve-connection! s programs {})
                                    (catch Exception _ nil)))
                         (.setDaemon true)
                         (.start)))
                     (catch Exception _ nil))))
        thread (doto (Thread. accept) (.setDaemon true) (.start))]
    {:port (.getLocalPort server)
     :dir dir
     :stop! (fn []
              (reset! running false)
              (try (.close server) (catch Exception _ nil))
              (.interrupt ^Thread thread)
              nil)}))
