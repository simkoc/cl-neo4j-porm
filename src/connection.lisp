(in-package :de.tu-braunschweig.cs.ias.cl-neo4j-porm)


(defclass neo4j-connection ()
  ((host :initarg :host
         :initform (error "a host has to be provided"))
   (port :initarg :port
         :initform (error "a port has to be provided"))
   (user :initarg :user
         :initform (error "a user has to be provided"))
   (password  :initarg :password
         :initform (error "a password has to be provided"))
   (single-transaction-interface
    :initarg :single-transaction-interface
    :initform "db/data/transaction/commit"
    :reader single-transaction-interface
    :reader sti)))


(defmethod get-url ((connection neo4j-connection))
  (with-slots (host port single-transaction-interface)
      connection
    (FORMAT nil "http://~a:~a/~a" host port single-transaction-interface)))


(defmacro with-neo4j-connection ((connection-name &key user password (host "localhost") (port "7474")) &body body)
  `(let ((,connection-name (make-instance 'neo4j-connection
                                          :host ,host
                                          :port ,port
                                          :user ,user
                                          :password ,password)))
     ,@body))
