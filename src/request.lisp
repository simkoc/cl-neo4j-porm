(in-package :de.tu-braunschweig.cs.ias.cl-neo4j-porm)


(defclass statement ()
  ((statement :initarg :statement
              :reader statement)
   (parameters :initarg :parameters
               :reader parameters)
   (resultdatacontentshastobeunique :initform '("row" "graph")
                                    :initarg :resultdatacontents)))


(defun replace-result-data-contents-in-json (string)
  (cl-ppcre:regex-replace "resultdatacontentshastobeunique" string "resultDataContents"))


(defclass statements ()
  ((statements
    :initarg :statements
    :reader statements)))


(defun make-requestable-query (query parameters graph-p)
  (make-instance 'statements
                 :statements (list (make-instance 'statement
                                                  :statement query
                                                  :parameters (create-hash-table parameters)
                                                  :resultdatacontents (if graph-p
                                                                          '("graph")
                                                                          '("row"))))))


(defmethod query-request ((connection neo4j-connection) (statements statements) graph-p)
  (with-slots (user password)
      connection
    (let ((ret-string (utf-8-bytes-to-string
                       (http-request (get-url connection)
                                     :method :post
                                     :basic-authorization (list user password)
                                     :content-type "application/json"
                                     :accept "application/json;charset=UTF-8"
                                     :content (replace-result-data-contents-in-json
                                               (with-output-to-string (stream)
                                                 (encode-json statements stream)))))))
      (with-input-from-string (stream ret-string)
        (parse-results (decode-json stream) t)))))


(defmethod query-request-raw ((connection neo4j-connection) (statements statements) graph-p)
  (declare (ignore graph-p))
  (format t "~a~%" (replace-result-data-contents-in-json
                    (with-output-to-string (stream)
                      (encode-json statements stream))))
  (with-slots (user password)
      connection
    (let ((ret-string (utf-8-bytes-to-string
                       (http-request (get-url connection)
                                     :method :post
                                     :basic-authorization (list user password)
                                     :content-type "application/json"
                                     :accept "application/json;charset=UTF-8"
                                     :content (replace-result-data-contents-in-json
                                               (with-output-to-string (stream)
                                                 (encode-json statements stream)))))))
      (with-input-from-string (stream ret-string)
        (decode-json stream)))))


(defmethod simple-query ((connection neo4j-connection) statement &rest parameters)
  (query-request connection
                 (make-requestable-query statement parameters nil)))


(defmethod graph-query ((connection neo4j-connection) statement &rest parameters)
  (query-request connection
                 (make-requestable-query statement parameters t)
                 t))


(defmethod simple-query-raw ((connection neo4j-connection) statement &rest parameters)
  (query-request-raw connection
                     (make-requestable-query statement parameters nil)))


(defmethod graph-query-raw ((connection neo4j-connection) statement &rest parameters)
  (query-request-raw connection
                     (make-requestable-query statement parameters t)
                     t))
