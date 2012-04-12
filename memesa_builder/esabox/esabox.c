#include <stdio.h>
#include "esabox.h"

int open_index(void** index, char* file_name) {

}

int open_content(void** content, char* file_name) {

}

int open_map(void** map, char* file_name) {

}

char* map_file(char* fname, int* resFd, long* size) {
	struct stat statInfo;

	int fd = open(fname, O_RDONLY);
	if(fd == -1)
		return NULL;

	fstat(fd, &statInfo);
	long fsize =  (size_t) statInfo.st_size;
	*size = fsize;
	*resFd = fd;
	char* res = mmap(0,  fsize, PROT_READ, MAP_PRIVATE, fd, 0);
	return res;
}


int main(int argc, char* argv[]) {
    
}
