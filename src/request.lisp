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


(defclass statement ()
  ((statement :initarg :statement
              :reader statement)
   (parameters :initarg :parameters
               :reader parameters)))


(defclass statements ()
  ((statements
    :initarg :statements
    :reader statements)))


(defclass results ()
  ((rows :initarg :rows
         :reader rows)
   (errors :initarg :errors
           :reader errors)))


(defun flatten (list-of-lists)
  (if list-of-lists
      (append (car list-of-lists)
            (flatten (cdr list-of-lists)))
      nil))


(defmethod results ((results results) &key (flatten-p nil))
  (if flatten-p
      (flatten (slot-value results 'rows))
      (slot-value results 'rows)))


(defclass entity ()
  ((id :initarg :id
       :reader get-id)
   (type :initarg :type
         :reader get-type)
   (attribute-table :initarg :attribute-table)))


(defmethod print-object ((entity entity) stream)
  (with-slots (id type attribute-table)
      entity
    (let ((attribute-list (list)))
      (maphash #'(lambda (key value)
                   (push (list key value) attribute-list))
               attribute-table)
      (let ((string (format nil "(~a {~{~{~a:~a~}~^, ~}})" type attribute-list)))
        (format stream string)))))


(defun create-hash-table (element-list)
  (let ((table (make-hash-table :test 'equalp)))
    (mapcar #'(lambda (elem)
                (setf (gethash (if (numberp (car elem))
                                   (car elem)
                                   (string-downcase (format nil "~a" (car elem))))
                               table)
                          (cdr elem)))
            element-list)
    table))


(defmethod get-attribute (attribute-name (entity entity))
  (gethash attribute-name (slot-value entity 'attribute-table)))


(define-condition neo4j-entity-error (simple-error) ())


(defmethod get-value ((entity entity))
  (if (string= (slot-value entity 'type) "value")
      (gethash 0 (slot-value entity 'attribute-table))
      (error 'neo4j-entity-error
             :format-control "the given entity is not a value entity but a ~a entity"
             :format-arguments (list (slot-value entity 'type)))))


(defun parse-column (json-column json-meta)
  (make-instance 'entity
                 :attribute-table (if (listp json-column)
                                      (create-hash-table json-column)
                                      (create-hash-table (list (cons 0 json-column))))
                 :id (cdar json-meta)
                 :type (if (cdadr json-meta)
                           (cdadr json-meta)
                           "value")))


(defun parse-row (json-row)
  (mapcar #'parse-column (cdar json-row) (cdadr json-row)))


(defun parse-rows  (json-rows)
  (mapcar #'parse-row (cdadar json-rows)))


(define-condition neo4j-query-error (simple-error) ())


(defun parse-errors (json-error-list)
  (mapcar #'(lambda (json-error)
              (error 'neo4j-query-error
                     :format-control "~a~%~a"
                     :format-arguments (list (cdar json-error)
                                             (cdadr json-error))))
          json-error-list))


(defun parse-results (json-list)
  (make-instance 'results
                 :rows   (parse-rows (cdar json-list))
                 :errors (parse-errors (cdadr json-list))))


(defmethod print-object ((results results) stream)
  (with-slots (rows errors)
      results
    (format stream "~a~%~a~%" rows errors)))


(defmethod simple-query-request ((connection neo4j-connection) (statements statements))
  (with-slots (user password)
      connection
    (let ((ret-string (utf-8-bytes-to-string
                       (http-request (get-url connection)
                                     :method :post
                                     :basic-authorization (list user password)
                                     :content-type "application/json"
                                     :accept "application/json;charset=UTF-8"
                                     :content (with-output-to-string (stream)
                                                (encode-json statements stream))))))
      (with-input-from-string (stream ret-string)
        (parse-results (decode-json stream))))))



(defmethod simple-query ((connection neo4j-connection) statement &rest parameters)
  (simple-query-request connection
                (make-instance 'statements
                               :statements (list
                                            (make-instance 'statement
                                                           :statement statement
                                                           :parameters (create-hash-table parameters))))))


#|
(with-neo4j-connection (con :user "neo4j" :password "123456")
  (simple-query con "MATCH (node)-[d:DYNAMIC_FUNCTION_NAME]->(m) WHERE id(node) = {node_id} RETURN node,d, m" (cons "node_id" 3921393)))

(with-neo4j-connection (con :user "neo4j" :password "123456")
  (simple-query con
                "MATCH (param)-[:PARENT_OF]->(name:AST {type:'string'})
                 WHERE id(param) = {param_id_i} OR
                       id(param) = {param_id_ii}
                 RETURN name.code, name.lineno"
                (cons "param_id_i" 179)
                (cons "param_id_ii" 190)))
|#

