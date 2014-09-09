#!/usr/bin/python

import MySQLdb
import struct
import sys
import os

class PrepareIndex:
    FN_VECTORS = "vectors"
    FN_VECTORS_NDX = "vectors.ndx"
    FN_2INDEX_TXT = "2index.txt"
    FN_2INDEX_TXT_SORTED = "2index.sorted"
    FN_2INDEX = "2index"
    FN_2INDEX_NDX = "2index.ndx"
    FN_2INDEX_CMAP = "2index.cmap"
    
    # vector length in the database (100 dimensions
    # + size information)
    VECTOR_SIZE = 100
    VECTOR_LENGTH = VECTOR_SIZE * 8 + 4 

    sort_mem_size = "2000M"

    db_index_name = "index_all2"

    def __init__(self, index_table, host, user, passwd, db, port = 3306):
    	print host, user, port, passwd, db
        conn = MySQLdb.connect (host = host,
                                port = port,
                                user = user,
                                passwd = passwd,
                                db = db)
        self.conn = conn
        self.cursor = conn.cursor ()
        self.last_id = 0
        self.db_index_name = index_table

    
    
    def prepare(self, ndx_path):
        # start from the beginning
        self.last_id = 0
        
        path = ndx_path
        try:
            os.makedirs(path)
        except:
            pass
        
        f_vectors = open(os.path.join(path, self.FN_VECTORS), "w")
        f_vectors_ndx = open(os.path.join(path, self.FN_VECTORS_NDX), "w")
        f_txt_2index = open(os.path.join(path, self.FN_2INDEX_TXT), "w")

        # load data from the database
        self.load_db()
        cntr = 0
	vector = ""
        doc_cnt = self.get_doc_count()
        curr_perc = 0.0

        concept_dict = {}

        print '> Building vectors & 2index'
        # for each row, write it to the file
        while 1:
            # show curr percentage if changed
            if curr_perc + 1 < float(cntr) / doc_cnt * 100:
                curr_perc = float(cntr) / doc_cnt * 100
                print "[%.2f %%] Processing doc #%d" % (curr_perc, cntr,)

            cntr += 1

            row = self.cursor.fetchone()
            if row is None:
                self.load_db()
                row = self.cursor.fetchone()
                if row is None:
                    break
            self.last_id = row[1]
            
            # read values
            doc_id = row[1]
            vector = row[3]
            concept_dict[doc_id] = True

            # read the vector
            s = row[3]
            s_len, = struct.unpack('>i', s[:4])
            s = s[4:]
            
            for i in range(s_len):
                concept_id, concept_val = struct.unpack('>if', s[:8])
                f_txt_2index.write("%d\t%d\n" % (concept_id, doc_id))
                s = s[8:]

	    if len(vector) != self.VECTOR_LENGTH:
	    	vector = vector + "\0" * (self.VECTOR_LENGTH - len(vector))
            f_vectors.write(vector)
	    f_vectors_ndx.write(struct.pack("i", doc_id))
            
        # close files
        f_vectors.close()
        f_vectors_ndx.close()
        f_txt_2index.close()

        # sort 2index
        try:
            os.makedirs("__tmp_prepare")
        except:
            pass

        # define output filenames
        f2indextxt = os.path.join(path, self.FN_2INDEX_TXT)
        f2indexsorted = os.path.join(path, self.FN_2INDEX_TXT_SORTED)
        f2index = os.path.join(path, self.FN_2INDEX)
        f2indexndx = os.path.join(path, self.FN_2INDEX_NDX)
        f2indexcmap = os.path.join(path, self.FN_2INDEX_CMAP)
        
        concept_count = len(concept_dict.keys())
        vector_size = self.VECTOR_SIZE
                           
        print '[done] # of concepts:', concept_count
        print '[done] vector size:', self.VECTOR_SIZE
        
        print '> Sorting 2index'
        # sort should get the --parallel parameter, but not all sorts can do that
        os.system("sort -n -T __tmp_prepare/ -S %s -o %s %s" % 
                  (self.sort_mem_size, f2indexsorted, f2indextxt))
        
        print '> Building binary 2index'
        os.system('./builder %s %s %s %s  %d %d' % 
                  (f2indexsorted, f2index, f2indexndx, f2indexcmap,
                   concept_count, self.VECTOR_SIZE, ))

        print '> Done.'
        

    def load_db(self):
        self.cursor.execute ("SELECT * FROM %s WHERE doc_id > %%s ORDER BY doc_id LIMIT 1000" % self.db_index_name, [ self.last_id ])

    def get_doc_count(self):
        self.cursor.execute("SELECT COUNT(*) FROM %s" % self.db_index_name)
        res = self.cursor.fetchone()
        return res[0]
    
    def load_ndx(self):
        self.cursor.execute("SELECT * FROM %s" % self.db_index_name)

if __name__ == "__main__":
    # check the user inputted parameters
    if len(sys.argv) != 2:
        print 'wrong number of parameters, exiting'
        exit(1)
    
    # prepare the txt file
    index_name = sys.argv[1]
    e = PrepareIndex(index_table = sys.argv[1],
                     host = "localhost", 
                     user = "root", 
                     passwd = "quaeCi9f", 
                     db = "esa_index")
    e.prepare(index_name)
    
