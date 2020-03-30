(in-package :de.tu-braunschweig.cs.ias.cl-neo4j-porm)


(defclass results ()
  ((errors :initarg :errors
           :reader errors)))


(defmethod print-object ((results results) stream)
  (with-slots (errors)
      results
    (format stream "~a~%" errors)))


(defclass row-results (results)
  ((rows :initarg :rows
         :reader rows)))


(defmethod print-object ((results row-results) stream)
  (with-slots (rows errors)
      results
    (format stream "~a~%~a~%" rows errors)))


(defclass graph-results (results)
  ((graphs :initarg :graphs
           :reader graphs)))


(defun flatten (list-of-lists)
  (if list-of-lists
      (append (car list-of-lists)
            (flatten (cdr list-of-lists)))
      nil))


(defmethod results ((results row-results) &key (flatten-p nil))
  (if flatten-p
      (flatten (slot-value results 'rows))
      (slot-value results 'rows)))


(defmethod results ((results graph-results) &key (flatten-p nil))
  (when flatten-p
    (warn "using flatten-p in results for graph-results has no effect"))
  (slot-value results 'graphs))


(defun parse-column (json-column json-meta)
  (cond
    ((string= (cdadr json-meta) "node")
     (make-node-entity-using-meta json-column json-meta))
    ((string= (cdadr json-meta) "relationship")
     (make-relationship-entity-using-meta json-column json-meta))
    (T
     (make-value-entity json-column json-meta))))


(defun parse-row (json-row)
  (mapcar #'parse-column (cdar json-row) (cdadr json-row)))


(defun parse-graph (graph-element)
  (make-instance 'graph
                 :nodes (mapcar #'make-node-entity (nth 0 graph-element))
                 :relationships (mapcar #'make-relationship-entity (nth 1 graph-element))))


(defun parse-data (data-list)
  (mapcar (lambda (data-element)
            (cond
              ((and (= (length data-element) 2)
                    (string= (car (nth 0 data-element)) "ROW")
                    (string= (car (nth 1 data-element)) "META"))
               (parse-row data-element))
              ((and (= (length data-element) 1)
                    (string= (car (nth 0 data-element)) "GRAPH"))
               (parse-graph data-element))
              (T
               (error (format nil "do not know what to do with data-element ~a" data-element)))))
          data-list))


(defun parse-results (json-list)
  (multiple-value-bind (rows graphs)
      (parse-data (cdar json-list))
    (if rows
        (make-instance 'row-results
                       :rows rows
                       :errors (parse-errors (cdadr json-list)))
        (make-instance 'graph-results
                       :graphs graphs
                       :errors (parse-errors (cdadr json-list))))))


(define-condition neo4j-query-error (simple-error) ())


(defun parse-errors (json-error-list)
  (mapcar #'(lambda (json-error)
              (error 'neo4j-query-error
                     :format-control "~a~%~a"
                     :format-arguments (list (cdar json-error)
                                             (cdadr json-error))))
          json-error-list))
