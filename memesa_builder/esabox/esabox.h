
typedef struct {
    int length;
    int items[];
} index_t;

#pragma pack(1)
typedef struct {
    int id;
    float score;
} vector_item_t __attribute__ ((packed))  __attribute__((aligned (1)));

typedef struct {
    int length;
    vector_item_t items[];
} vector_t  __attribute__ ((packed)) __attribute__((aligned (1)));
#pragma pack(0)

typedef struct {
  int term_id;
  float term_idf;
} idf_map_item_t;

typedef struct {
  int term_id;
  int vector_id;
} vector_map_item_t;

typedef idf_map_item_t idf_map_t[];

typedef vector_map_item_t vector_map_t[];

typedef char* term_strings_t;
typedef vector_t* term_vectors_t;

vector_t* grow_vector(vector_t* curr_vector) {
  vector_t* res;
  if(curr_vector == NULL) {
    res = (vector_t*) malloc(sizeof(vector_t) + sizeof(vector_item_t) * 1000);
    res->length = 1000;
  } else {
    curr_vector->length *= 2;
    res = (vector_t*) realloc(curr_vector, sizeof(vector_t) + 
			      sizeof(vector_item_t) * curr_vector-> length);
  }
  return res;
}

//#define vector_item_t __attribute__((__packed__))
//#define vector_t __attribute__((__packed__))
