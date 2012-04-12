CFLAGS = -I../hashtable -g3 -lm
mysql_CFLAGS = $(CFLAGS) $$(mysql_config --cflags) -shared -fPIC -g3

SEARCHER_BIN = searcher
BUILDER_BIN = builder
MYSQL_PLUGIN_SONAME = esasearch_mysql.so

OUTPUT = $(MYSQL_PLUGIN_SONAME) $(SEARCHER_BIN) $(BUILDER_BIN)

.PHONY : all
all: $(OUTPUT)

.PHONY : clean
clean:
	rm -f $(OUTPUT)

$(SEARCHER_BIN): searcher.c searcher.h ../hashtable/hashtable.o bits.h

$(BUILDER_BIN): builder.c

.PHONY : mysql_plugin
mysql_plugin: $(MYSQL_PLUGIN_SONAME)

mysql_plugin_install:
	sudo cp $(MYSQL_PLUGIN_SONAME) /usr/lib/mysql/plugin/

$(MYSQL_PLUGIN_SONAME): searcher_mysql.c searcher.c searcher.h bits.h ../hashtable/hashtable.o
	$(CC) $(mysql_CFLAGS) -o $@ $^ 

../hashtable/hashtable.o: ../hashtable/hashtable.c
	cd ../hashtable/; $(MAKE) hashtable.o
