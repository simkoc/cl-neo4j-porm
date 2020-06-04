(in-package :de.tu-braunschweig.cs.ias.cl-neo4j-porm)


;; conditions

(define-condition neo4j-porm-condition (condition)
  ())


(define-condition node-mismatch-condition (neo4j-porm-condition)
  ((node-a :initarg :node-a)
   (node-b :initarg :node-b)
   (mismatch-explanation :initarg :mismatch-explanation)))


(defmethod print-object ((condition node-mismatch-condition) stream)
  (with-slots (node-a node-b mismatch-explanation)
      condition
    (format stream "mistmatch between ~a and ~a: ~a" node-a node-b mismatch-explanation)))

;; parsing utility


(defun create-hash-table (element-list)
  (let ((table (make-hash-table :test 'equalp)))
    (mapcar #'(lambda (elem)
                (setf (gethash (if (numberp (car elem))
                                   (car elem)
                                   (string-downcase (format nil "~a" (car elem))))
                               table)
                          (remove #\~ (cdr elem))))
            element-list)
    table))


;; class definitions


(defclass entity () ())


(defgeneric get-entity-type (entity)
  (:documentation "returns the type of the entity"))


(defgeneric print-object (entity stream)
  (:documentation "print the entity object"))


(defgeneric get-property (attribute-name entity)
  (:documentation "get the attribute attribute-name from the entity"))


(defmethod print-object ((entity entity) stream)
  (format stream "(entity)"))


(defclass value (entity)
  ((value :initarg :value
          :reader value)))


(defun make-value-entity-using-meta (json-column json-meta)
  (declare (ignore json-meta))
  (make-instance 'value
                 :value json-column))


(defmethod print-object ((entity value) stream)
  (format stream "<~a>" (value entity)))


(defmethod get-value ((entity value))
  (warn "get-value is deprecated and will be dropped in future versions of cl-neo4j-porm")
  (value entity))


(defmethod get-entity-type ((entity value))
  :value)


(defclass graph-entity (entity)
  ((properties :initarg :properties
               :reader properties)
   (id :initarg :id
       :reader id)))


(defmethod entity= ((node-lhs graph-entity) (node-rhs graph-entity) &rest nodes)
  (labels ((helper (a b rem)
             (if rem
                 (and
                  (= (id a) (id b))
                  (helper a (car rem) (cdr rem)))
                 (= (id a) (id b)))))
    (helper node-lhs node-rhs nodes)))

(defmethod get-property (property (entity graph-entity))
  (gethash property (slot-value entity 'properties)))


(defmethod get-attribute (property (entity graph-entity))
  (warn "get-attribute is deprecated and will be dropped in future versions of cl-neo4j-porm")
  (get-property property entity))


(defmethod get-id ((entity graph-entity))
  (warn "get-id is deprecated and will be dropped in future versions of cl-neo4j-porm")
  (id entity))


(defclass node (graph-entity)
  ((label :initarg :label
          :initform nil
          :reader label)
   (relationships<- :initarg :relationships<-
                     :initform (list))
   (relationships-> :initarg :relationships->
                      :initform (list))))


(defmethod forget-graph-position ((node node))
  (setf (slot-value node 'relationship<-) nil)
  (setf (slot-value node 'relationship->) nil))


(defmethod relationships-> ((node node) &key limit-to)
  (with-slots (relationships->)
      node
    (if limit-to
        (remove-if-not (lambda (rel)
                         ;(format t "~a~%" (find (rel-type rel) limit-to :test #'string=))
                         (find (rel-type rel) limit-to :test #'string=))
                       relationships->)
        relationships->)))


(defmethod relationships<- ((node node) &key limit-to)
  (with-slots (relationships<-)
      node
    (if limit-to
        (remove-if-not (lambda (rel)
                         (find (rel-type rel) limit-to :test #'string=))
                       relationships<-)
        relationships<-)))


(defmethod sync-nodes ((node-a node) (node-b node))
  (when (not (= (id node-a) (id node-b)))
    (error 'node-mismatch-condition
           :node-a node-a
           :node-b node-b
           :mismatch-explanation "they have to have the same id"))
  (let ((relationships-> (append (relationships-> node-a)
                                 (relationships-> node-b)))
        (relationships<- (append (relationships<- node-a)
                                 (relationships<- node-b))))
    (setf (slot-value node-a 'relationships->) relationships->)
    (setf (slot-value node-a 'relationships<-) relationships<-)
    (setf (slot-value node-b 'relationships->) relationships->)
    (setf (slot-value node-b 'relationships<-) relationships<-))
  (values node-a node-b))


(defmethod relationships->* ((node node) (neo4j neo4j-connection) &key limit-to force-update)
  (when (not (listp limit-to))
    (setf limit-to (list limit-to)))
  (let ((rels (relationships-> node :limit-to limit-to)))
    (if (or (not rels)
            force-update)
        (let ((result (graphs (graph-query neo4j (format nil "MATCH (node)-[con]->(where)
                                                              WHERE id(node) = {start_node} AND
                                                              ~a
                                                              RETURN node, con, where"
                                                         (if limit-to
                                                             "type(con) IN {limit_to}"
                                                             "1 = 1"))
                                           (cons "start_node" (id node))
                                           (cons "limit_to" limit-to)))))
          (if (= (length result) 0)
              nil
              (progn
                (sync-nodes node (node (id node) (apply #'merge-graphs (car result) (cdr result))))
                (relationships-> node  :limit-to limit-to))))
        rels)))


(defmethod relationships<-* ((node node) (neo4j neo4j-connection) &key limit-to force-update)
  (when (not (listp limit-to))
    (setf limit-to (list limit-to)))
  (let ((rels (relationships<- node :limit-to limit-to)))
    (if (or (not rels)
            force-update)
        (let ((result (graphs (graph-query neo4j (format nil "MATCH (node)<-[con]-(where)
                                                              WHERE id(node) = {start_node} AND
                                                              ~a
                                                              RETURN node, con, where"
                                                         (if limit-to
                                                             "type(con) IN {limit_to}"
                                                             "1 = 1"))
                                           (cons "start_node" (id node))
                                           (cons "limit_to" limit-to)))))
          (if (= (length result) 0)
              nil
              (progn
                (sync-nodes node (node (id node) (apply #'merge-graphs (car result) (cdr result))))
                (relationships<- node  :limit-to limit-to))))
        rels)))



(defmethod has-label-p ((entity node) label)
  (if (find label (label entity) :test #'string=)
      T
      nil))


(defmethod print-object ((entity node) stream)
  (with-slots (id label properties)
      entity
    (let ((properties-list (list)))
      (maphash #'(lambda (key value)
                   (push (list key value) properties-list))
               properties)
      (let ((string (format nil "(:~a {~{~{~a:~a~}~^, ~}})" label properties-list)))
        (format stream string)))))


(defun make-node-entity-using-meta (json-column json-meta)
  (make-instance 'node
                 :label ""
                 :properties (if (listp json-column)
                                 (create-hash-table json-column)
                                 (create-hash-table (list (cons 0 json-column))))
                 :id (cdar json-meta)))


(defun make-node-entity (json-column)
  (destructuring-bind (id labels properties)
      json-column
    (assert (listp (cdr properties)))
    (make-instance 'node
                   :id (parse-integer (cdr id))
                   :label (cdr labels)
                   :properties (create-hash-table (cdr properties)))))


(defmethod get-entity-type ((entity node))
  :node)


(defclass relationship (graph-entity)
  ((start :initarg :start
          :reader start)
   (end :initarg :end
        :reader end)
   (rel-type :initarg :rel-type
             :reader rel-type)))


(defmethod initialize-instance :after ((rel relationship) &rest rest)
  (declare (ignore rest))
  (with-slots (start end)
      rel
    (assert (and start end))))


(defmethod print-object ((entity relationship) stream)
  (with-slots (id rel-type properties start end)
      entity
    (let ((properties-list (list)))
      (maphash #'(lambda (key value)
                   (push (list key value) properties-list))
               properties)
      (if (and (typep start 'node)
               (typep end 'node))
          (let ((string (format nil "(~a)-[:~a {~{~{~a:~a~}~^, ~}}]-(~a)" (id start) rel-type properties-list (id end))))
            (format stream string))
          (let ((string (format nil "[:~a {~{~{~a:~a~}~^, ~}}]" rel-type properties-list)))
            (format stream string))))))


(defun make-relationship-entity-using-meta (json-column json-meta)
  (make-instance 'relationship
                 :properties (if (listp json-column)
                                 (create-hash-table json-column)
                                 (create-hash-table (list (cons 0 json-column))))
                 :rel-type ""
                 :start -1
                 :end -1
                 :id (cdar json-meta)))


(defun make-relationship-entity (json-column)
  (destructuring-bind (id type start-node end-node properties)
      json-column
    (assert (listp (cdr properties)))
    (make-instance 'relationship
                   :id (parse-integer (cdr id))
                   :rel-type (cdr type)
                   :start (parse-integer (cdr start-node))
                   :end (parse-integer (cdr end-node))
                   :properties (create-hash-table (cdr properties)))))


(defmethod get-type ((entity relationship))
  :relationship)


(defclass graph ()
  ((nodes :initarg :nodes
          :reader nodes)
   (relationships :initarg :relationships
                  :reader relationships)
   (node-index :initform (make-hash-table))
   (relationship-index :initform (make-hash-table))))


(defmethod node ((id integer) (graph graph))
  (gethash id (slot-value graph 'node-index)))


(defmethod relationship ((id integer) (graph graph))
  (gethash id (slot-value graph 'relationship-index)))


(defmethod initialize-graph-structure ((graph graph))
  (with-slots (nodes relationships node-index relationship-index)
      graph
    (mapcar (lambda (node)
              (assert (integerp (id node)))
              (setf (gethash (id node) node-index) node))
            nodes)
    (mapcar (lambda (relationship)
              (with-slots (id start end)
                  relationship
                (assert (and (integerp start) (integerp end)))
                (setf (gethash id relationship-index) relationship)
                (push relationship (slot-value (gethash start node-index) 'relationships->))
                (setf start (gethash start node-index))
                (push relationship (slot-value (gethash end node-index) 'relationships<-))
                (setf end (gethash end node-index))))
            relationships)))


(defmethod initialize-instance :after ((graph graph) &rest stuff)
  (declare (ignore stuff))
  (initialize-graph-structure graph))


(defmethod delete-relationships-* ((node node))
  (setf (slot-value node 'relationships->) nil)
  (setf (slot-value node 'relationships<-) nil)
  node)


(defmethod revert-relationship-links-* ((rel relationship))
  (if (not (integerp (slot-value rel 'start)))
      (setf (slot-value rel 'start) (id (slot-value rel 'start))))
  (if (not (integerp (slot-value rel 'end)))
      (setf (slot-value rel 'end) (id (slot-value rel 'end))))
  rel)


(defmethod merge-graphs ((graph graph) &rest graphs)
  (if (not graphs)
      graph
      (progn
        (assert (typep (car graphs) 'graph))
        (apply #'merge-graphs (make-instance 'graph
                                             :nodes (mapcar #'delete-relationships-* (append (nodes graph)
                                                                                             (nodes (car graphs))))
                                             :relationships (mapcar #'revert-relationship-links-*
                                                                    (append (relationships graph)
                                                                            (relationships (car graphs)))))
               (cdr graphs)))))


(defmethod print-object ((graph graph) stream)
  (with-slots (nodes relationships)
      graph
    (format stream "<Graph~%~{~a~%~}~{~a~^~%~}>"
            nodes relationships)))
