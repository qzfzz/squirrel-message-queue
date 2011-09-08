#ifndef SLABS_H
#define SLABS_H

unsigned int slabs_clsid(unsigned int size);
void slabs_init(unsigned int limit);
int slabs_newslab(unsigned int id);
void *slabs_alloc(unsigned int size);
void slabs_free(void *ptr, unsigned int size);
char* slabs_stats(int *buflen);

#endif
