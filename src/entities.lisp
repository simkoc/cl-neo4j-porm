(in-package :de.tu-braunschweig.cs.ias.cl-neo4j-porm)


;; parsing utility


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


;; class definitions


(defclass entity () ())


(defgeneric get-entity-type (entity)
  (:documentation "returns the type of the entity"))


(defgeneric print-object (entity stream)
  (:documentation "print the entity object"))


(defgeneric get-attribute (attribute-name entity)
  (:documentation "get the attribute attribute-name from the entity"))


(defmethod get-attribute (attribute-name (entity entity))
  (gethash attribute-name (slot-value entity 'attribute-table)))


(defmethod print-object ((entity entity) stream)
  (with-slots (id attribute-table)
      entity
    (let ((attribute-list (list)))
      (maphash #'(lambda (key value)
                   (push (list key value) attribute-list))
               attribute-table)
      (let ((string (format nil "(~a {~{~{~a:~a~}~^, ~}})" (get-type entity) attribute-list)))
        (format stream string)))))


(defclass value (entity)
  ((value :initarg :value
          :reader value)))


(defun make-value-entity (json-column json-meta)
  (make-instance 'value
                 :attribute-table (if (listp json-column)
                                      (create-hash-table json-column)
                                      (create-hash-table (list (cons 0 json-column))))
                 :id (cdar json-meta)))


(defmethod get-entity-type ((entity value))
  :value)


(defclass graph-entity (entity)
  ((properties :initarg :properties
               :reader properties)
   (id :initarg :id
       :reader id)))


(defclass node (graph-entity)
  ((label :initarg :label
          :initform nil
          :reader label)
   (in-relationships :initarg :relationships<-
                     :initform (list)
                     :reader relationships<-)
   (out-relationships :initarg :relationships->
                      :initform (list)
                      :reader relationships->)))


(defun make-node-entity-using-meta (json-column json-meta)
  (make-instance 'node
                 :attribute-table (if (listp json-column)
                                      (create-hash-table json-column)
                                      (create-hash-table (list (cons 0 json-column))))
                 :id (parse-integer (cdar json-meta))))


(defun make-node-entity (json-column)
  (make-instance 'node
                 :id (parse-integer (cdr (nth 0 json-column)))
                 :label (cdr (nth 1 json-column))
                 :properties (progn (assert (listp (cdr (nth 2 json-column))))
                                    (create-hash-table (cdr (nth 2 json-column))))))


(defmethod get-entity-type ((entity node))
  :node)


(defclass relationship (entity)
  ((start :initarg :start
          :reader start)
   (end :initarg :end
        :reader end)
   (rel-type :initarg :type
             :reader rel-type)
   (properties :initarg :properties
               :reader properties)))


(defun make-relationship-entity-using-meta (json-column json-meta)
  (make-instance 'relationship
                 :attribute-table (if (listp json-column)
                                      (create-hash-table json-column)
                                      (create-hash-table (list (cons 0 json-column))))
                 :id (parse-integer (cdar json-meta))))


(defun make-relationship-entity (json-column)
  (make-instance 'node
                 :id (parse-integer (cdr (nth 0 json-column)))
                 :type (cdr (nth 1 json-column))
                 :start (parse-integer (cdr (nth 2 json-column)))
                 :end (parse-integer (cdr (nth 3 json-column)))
                 :properties (progn (assert (listp (cdr (nth 2 json-column))))
                                    (create-hash-table (cdr (nth 2 json-column))))))


(defmethod get-type ((entity relationship))
  :relationship)


(defclass graph ()
  ((nodes :initarg :nodes
          :reader nodes)
   (relationships :initarg :relationships
                  :reader relationships)
   (node-index :initform (make-hash-table))
   (relationship-index :initform (make-hash-table))))


(defmethod initialize-instance :after ((graph graph) &rest stuff)
  (declare (ignore stuff))
  (with-slots (nodes relationships node-index relationship-index)
      graph
    (mapcar (lambda (node)
              (setf (gethash (id node) node-index) node))
            nodes)
    (mapcar (lambda (relationship)
              (with-slots (id start end)
                  relationship
                (setf (gethash id relationship-index) relationship)
                (push relationship (slot-value (gethash start node-index) 'relationships->))
                (setf start (gethash start node-index))
                (push relationship (slot-value (gethash end node-index) 'relationship<-))
                (setf end (gethash end node-index))))
            relationships)))
