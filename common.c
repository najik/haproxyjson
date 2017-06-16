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

#define COMMON_C
#include "common.h"

#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>

#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <time.h>

struct common_options g_common_options = { false, true, true };

/* timestamp */
int strtime(char *str, size_t size) {
	time_t t;
	struct tm lt;

	time( &t );
	localtime_r( &t, &lt );

	return strftime( str, size, "%Y-%m-%d %H:%M:%S", &lt );
}

/* generic stderr printf with timestamp */
int verrf( const char *fmt, va_list ap ) {
	int ret;
	char timebuf[64], *newfmt;
	
	strtime( timebuf, sizeof( timebuf ) );

	newfmt = malloc( strlen(fmt) + strlen(timebuf) + 16 );
	strcpy( newfmt, timebuf );
	strcat( newfmt, " :: " );
	strcat( newfmt, fmt );
	strcat( newfmt, "\n" );

	ret = vfprintf( stderr, newfmt, ap );

	free( newfmt );
	return ret;
}

int errf( const char *fmt, ... ) {
	va_list ap;
	int ret; 

	va_start( ap, fmt );
	ret = verrf( fmt, ap );
	va_end( ap );

	return ret;
}

/* error printf */
int errorf( const char *fmt, ... ) {
	va_list ap;
	int ret;
	if(!g_common_options.error)
		return 0;
	va_start( ap, fmt );
	ret = verrf( fmt, ap );
	va_end( ap );
	return ret;
}

/* info printf */
int infof( const char *fmt, ... ) {
	va_list ap;
	int ret;
	if(!g_common_options.info)
		return 0;
	va_start( ap, fmt );
	ret = verrf( fmt, ap );
	va_end( ap );
	return ret;
}

/* debug printf */
int debugf( const char *fmt, ... ) {
	va_list ap;
	int ret;
	if(!g_common_options.debug)
		return 0;
	va_start( ap, fmt );
	ret = verrf( fmt, ap );
	va_end( ap );
	return ret;
}

void errstop(const char * s) {
	errf( "%s: %s\n", s, strerror( errno ) );
	exit( 1 );
}

/* add a buffer to a pool */
struct buffer* buffer_pool_add(struct buffer **pool, size_t size) {
	struct buffer **p, *pnew;
	size_t pow2 = 1;
	struct timespec ts;

	/* round up buffer size to next power of 2. TODO: make better  */
	while (pow2 < size)
		pow2 <<= 1;
	size = pow2;

	pnew = malloc( sizeof( struct buffer ) );
	pnew->size = size;
	pnew->len = 0;
	pnew->buf = malloc( size );
	pnew->release = 0;
	pnew->next = NULL;

	clock_gettime( CLOCK_REALTIME, &ts );
	memcpy( &pnew->time_acquired, &ts, sizeof( ts ) );
	memcpy( &pnew->time_released, &ts, sizeof( ts ) );

	for( p = pool; *p; p = &(*p)->next )
		;
	(*p) = pnew;
	return pnew;
}

/* acquire an available buffer from a pool, add if necessary */
struct buffer* buffer_pool_acquire(struct buffer **pool, size_t size)
{
	/* TODO: Find _smallest_ buffer that meets caller's requirements */
	struct buffer **p;
	for( p = pool; *p; p=&(*p)->next ) {
		if( (*p)->release && (*p)->size >= size ) {
			(*p)->release = false;
			(*p)->len = 0;
			clock_gettime( CLOCK_REALTIME, &(*p)->time_acquired );
			return *p;
		}

	}
	return buffer_pool_add( pool, size );
}

/* release a buffer from the pool, make it available for reuse */
void buffer_pool_release(struct buffer *buf) {
	buf->release = true;
	clock_gettime( CLOCK_REALTIME, &buf->time_released );
}

/* free unused pools by last time used */
void buffer_pool_gc_by_age(struct buffer **pool, time_t seconds) {
	struct buffer **p, *del;
	struct timespec ts;

	clock_gettime( CLOCK_REALTIME, &ts );

	p = pool;
	while( *p ) {
		if( (*p)->release && ( ts.tv_sec - (*p)->time_released.tv_sec > seconds ) ) {
			del = *p;
			free( del->buf );
			del->buf = 0;
			*p = (*p)->next;
			free( del );
		} else 
			p = &(*p)->next;
	}
}

/* free all unused pools */
void buffer_pool_gc(struct buffer **pool) {
	struct buffer **p, *del;

	p = pool;
	while( *p ) {
		if( (*p)->release ) {
			del = *p;
			free( del->buf );
			del->buf = 0;
			*p = (*p)->next;
			free( del );
		} else 
			p = &(*p)->next;
	}
}

size_t buffer_pool_count( struct buffer *p ) {
	size_t length = 0;
	for( ; p; p = p->next )
		length++;
	return length;
}

size_t buffer_pool_size( struct buffer *p ) {
	size_t size = 0;
	for( ; p; p = p->next )
		size += p->size + sizeof( struct buffer );
	return size;
}

/* remove a number of bytes from the beginning of a buffer, shift the remaining contents up */
void buffer_pop( struct buffer *buffer, size_t numbytes ) {
	debugf( "buffer_pop: removing %lu, previous length: %lu", numbytes, buffer->len );
	if( numbytes > buffer->len ) {
		debugf( "buffer_pop: removing: %lu, length: %lu", numbytes, buffer->len );
		numbytes = buffer->len;
	}
	/* TODO: optimized case where there is going to be nothing in the buffer, just set len to 0 */
	memmove(buffer->buf, buffer->buf + numbytes, buffer->len - numbytes);
	buffer->len -= numbytes;
	debugf( "buffer_pop: new length: %lu", buffer->len );
}

/* remove a newline delimited line from the top of the buffer, return it. caller responsible to free the string */
char *buffer_getline( struct buffer *buffer ) {
	char *p = NULL, *s = NULL;
	size_t len = 0;
	if( (NULL == buffer) || NULL == ( p = memchr( buffer->buf, '\n', buffer->len ) ) )
		return NULL;
	len = p - buffer->buf + 1; /* +1 to include newline */
	s = malloc( len + 1 ); /* +1 for null terminate */
	debugf("buffer_getline: copying %lu bytes", len);
	memcpy( s, buffer->buf, len );
	s[len] = '\0';
	debugf("buffer_getline: [%s]", s);
	buffer_pop( buffer, len );
	return s;
}

/* append string into buffer, returns number of bytes copied to buffer */
size_t buffer_writes( struct buffer *buffer, const char *s ) {
	size_t sourcelen, avail, tocopy;
	sourcelen = strlen( s );
	avail = buffer->size - buffer->len;
	tocopy = ( avail < sourcelen ? avail : sourcelen );
	memcpy( buffer->buf + buffer->len, s, tocopy );
	buffer->len += tocopy;
	return tocopy;
}

/* read into buffer */
size_t buffer_pool_read(int desc, struct buffer *p, bool *eof) {
	char *buf = p->buf + p->len;
	size_t len = p->size - p->len;
	size_t got = 0;
	ssize_t ret;
	while (len) {
		ret = read(desc, buf, len);
		if (ret < 0) {
			if( errno != EAGAIN )
				fprintf(stderr, "readn: %s\n", strerror( errno ) );
			break;
		}
		if (ret == 0) {
			*eof = true;
			break;
		}
		buf += ret;
		len -= ret;
		got += ret;
		debugf( "read: %lu", ret );
	}
	debugf( "buffer_pool_read: %lu", got );
	p->len += got;
	return got;
}

/* queue a buffer for transmission */
struct sendqueue *send_queue_add_buffer(struct sendqueue **queue, struct buffer *buf) {
	struct sendqueue *qnew, **p;
	
	qnew = malloc( sizeof( struct sendqueue ) );
	qnew->buffer = buf;
	qnew->sent = 0;
	qnew->next = NULL;
	
	for( p = queue; *p; p = &(*p)->next )
		;
	(*p) = qnew;
	return qnew;
}

/* attempt to send a single sendqueue item
   return true if sending the buffer completed */
size_t send_queue_send( int sock, struct sendqueue *queue ) {
	char *buf;
	size_t len, sent=0;
	ssize_t ret;

	buf = queue->buffer->buf + queue->sent;
	len = queue->buffer->len - queue->sent;

	while( len ) {
		ret = send( sock, buf, len, 0 );
		if( ret < 0 ) {
			if( errno != EAGAIN )
				fprintf( stderr, "send: %s\n", strerror( errno ) );
			break;
		}
		debugf( "send: %lu bytes", ret );
		sent += ret;
		buf += ret;
		len -= ret;
	}
	queue->sent += sent;

	debugf("send_queue_send: %lu bytes", sent);

	return (sent);
}

size_t send_queue_transmit(int sock, struct sendqueue **queue) {
	struct sendqueue **p, *del;
	size_t sent = 0;

	p = queue;
	while( *p ) {
		sent += send_queue_send( sock, *p );
		if( (*p)->buffer->len == (*p)->sent ) {
			del = *p;
			buffer_pool_release( del->buffer );
			*p = (*p)->next;
			free( del );
			debugf( "send_queue_transmit: completed a block");
		} else {
			debugf( "send_queue_transmit: incomplete");
			break;
		}
	}
	return sent;
}

/* 
When we have hit our memory limit, we call this function to discard the oldest 
unsent buffer in the queue.  Called repeatedly until we are within our memory 
limit again.  We skip partially sent buffers because 
*/
size_t send_queue_discard( struct sendqueue **queue, size_t amount) {
	struct sendqueue **p, *del;
	size_t count = 0, total = 0, discarded;

	p = queue;
	/* TODO: fix this ugly hack */
	/* we check count % 2 because buffers are queued 2 at a time.  one for the command, one for the data.  we don't want to remove one
	without the other.  this is an ugly hack and needs to be resolved, probably by combining command and data into a single chunk. */
	while( *p && ( (total < amount) || ( count % 2 ) ) ) {
		/* skip partially sent buffers, avoid tearing */
		if( (*p)->sent ) {
			p = &(*p)->next;
			continue;
		}
		/* accounting */
		discarded = ( (*p)->buffer->size + sizeof( struct buffer ) + sizeof( struct sendqueue ) );
		total += discarded;
		count += 1;
		/* release the buffer, free the queue item */
		del = *p;
		buffer_pool_release( del->buffer );
		*p = (*p)->next;
		free( del );
		debugf( "send_queue_discard: discarded a block: %lu bytes", discarded );
	}
	return count;
}

size_t send_queue_size(struct sendqueue *p) {
	size_t count=0;
	for( ; p ; p=p->next )
		count += p->buffer->len - p->sent;
	return count;
}

size_t send_queue_allocated(struct sendqueue *p) {
	static const size_t overhead = sizeof( struct buffer ) + sizeof( struct sendqueue );
	size_t count=0;
	for( ; p ; p=p->next )
		count += p->buffer->size + overhead;
	return count;
}

size_t send_queue_count(struct sendqueue *p) {
	size_t count=0;
	for( ; p ; p=p->next )
		count++;
	return count;
}

/* make a file descriptor non-blocking */
int nonblock( int fd ) {
	int flags;

	flags = fcntl( fd, F_GETFL, 0); 
	if ( -1 == flags ) 
		errstop( "fcntl: get flags");
	flags |= O_NONBLOCK;
	if( -1 == fcntl( fd, F_SETFL, flags) )
		errstop( "fcntl: set flags");

	return 0;
}

/* read a word list from a file */
struct wordlist *wordlist_from_file( const char *filename ) {
	const char *delim = "\r\n ";
	struct stat st;
	struct wordlist *wl;
	int fd;
	char *tempdata, *saveptr=0;
	size_t wordcount=0, i=0, numread=0;
	ssize_t res;

	if( stat( filename, &st ) ) {
		errorf( "wordlist_from_file: stat: %s", strerror( errno ) );
		return NULL;
	}
	if( ! ( fd = open( filename, O_RDONLY ) ) ) {
		errorf( "wordlist_from_file: open: %s", strerror( errno ) );
	}
	wl = malloc( sizeof( struct wordlist ) );
	wl->data_size = st.st_size + 1; /* +1 for null terminator */
	wl->data = malloc( wl->data_size + 1 );
	bzero( wl->data, wl->data_size + 1 ); /* TODO: valgrind, why error if this isn't here? */

	while( ! ( res = read( fd, &wl->data[numread], wl->data_size - numread ) ) ) {
		if( res == -1 ) {
			errorf( "wordlist_from_file: read: ", strerror( errno ) );
			free(wl->data);
			free(wl);
			close( fd );
			return NULL;
		} 
		numread += res;
	}
	wl->data[ wl->data_size ] = 0;
	close( fd );
	
	tempdata = malloc( wl->data_size );
	memcpy( tempdata, wl->data, wl->data_size );
	while( strtok_r( wordcount == 0 ? tempdata : NULL, delim, &saveptr ) )
		++wordcount;
	free( tempdata );

	wl->ptr_size = wordcount;
	wl->ptrs = malloc( wordcount * sizeof( char * ) );

	for( i = 0; i < wl->ptr_size; i++ )
		wl->ptrs[i] = strtok_r( i == 0 ? wl->data : NULL, delim, &saveptr );

	return wl;
}

void wordlist_print( struct wordlist *wl ) {
	size_t i;
	for( i = 0; i < wl->ptr_size; i++)
		fprintf( stderr, "%lu %s\n", i, wl->ptrs[i] );
}


void wordlist_destroy( struct wordlist *wl ) {
	free( wl->ptrs );
	free( wl->data );
	free( wl );
}
