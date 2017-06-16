/*
The MIT License (MIT)

Copyright (c) 2015 Naji Al-Khudairi

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

#ifndef COMMON_H
#define COMMON_H

#include <stdlib.h>
#include <stdbool.h>
#include <stdarg.h>
#include <string.h>

struct common_options {
	bool	debug,
		error,
		info;
};

#ifndef COMMON_C
extern struct common_options g_common_options;
#endif

struct buffer {
	size_t 			size, 
				len;
	char 			*buf;
	bool 			release;
	struct timespec		time_acquired,
				time_released;
	struct buffer* 		next;
};

struct sendqueue {
	struct buffer *buffer;
	size_t sent;
	struct sendqueue *next;
};

struct wordlist {
	size_t data_size;
	char* data;
	size_t ptr_size;
	char** ptrs;
};


#ifndef WORDLIST_MATCH_ANY
#define WORDLIST_MATCH_ANY
static inline bool wordlist_match_any( struct wordlist *wl, const char *s ) {
	size_t i;
	for(i = 0; i < wl->ptr_size; ++i)
		if( ! strcasecmp( wl->ptrs[i], s ) )
			return true;
	return false;
}
#endif

/*
 * Conditional Formatted Output
 */

/* formatted debug output */
int debugf( const char *fmt, ... );
int infof( const char *fmt, ... );
int errorf( const char *fmt, ... );

/* fatal error output */
void errstop( const char * s );

/*
 * Buffers
 */

struct buffer* buffer_pool_acquire( struct buffer **pool, size_t size );
void buffer_pool_release( struct buffer *buf );
void buffer_pool_gc_by_age(struct buffer **pool, time_t seconds);
void buffer_pool_gc( struct buffer **pool );
size_t buffer_pool_count( struct buffer *p );
size_t buffer_pool_size( struct buffer *p );
void buffer_pop( struct buffer *buffer, size_t numbytes );
char *buffer_getline( struct buffer *buffer );
size_t buffer_writes( struct buffer *buffer, const char *s );

size_t buffer_pool_read( int desc, struct buffer *p, bool *eof );

struct sendqueue *send_queue_add_buffer( struct sendqueue **queue, struct buffer *buf );
size_t send_queue_transmit( int sock, struct sendqueue **queue );
size_t send_queue_discard( struct sendqueue **queue, size_t remaining);
size_t send_queue_size( struct sendqueue *p );
size_t send_queue_allocated(struct sendqueue *p);
size_t send_queue_count( struct sendqueue *p );

/*
 * Sockets
 */

int nonblock( int fd );

/*
 * Word Lists
 */

struct wordlist *wordlist_from_file( const char *filename );
void wordlist_print( struct wordlist *wordlist );
void wordlist_destroy( struct wordlist *wordlist );

#endif /* ifndef COMMON_H */
