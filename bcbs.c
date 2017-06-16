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
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>
#include <ctype.h>
#include <errno.h>
#include <unistd.h>
#include <signal.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <stdbool.h>
#include <time.h>
#include <zlib.h>

#include "common.h"

#define BUFSIZE 131072

const int protocol_version = 1;

const char g_commit[] = COMMIT;

struct options {
	char		*host; 	/* client */
	char 		*port,
			*opt_ip_file; /* server - optional ip whitelist */
	bool		listen,
			prefix, /* server */
			fl_sigint,	/* received SIGINT */
			fl_reload;	/* received SIGUSR1 to reload files */
	long		max_buffer_age; /* time until unused buffers are freed */
	size_t 		max_memory; /* maximum size of sendqueue allocations */
	struct metadata	*md; 	/* client */
	struct columndata *cd;	/* client */
	struct wordlist *iplist; /* server - ip whitelist */
};

struct options g_options;

enum client_state {
	CLIENT_WAIT_HEADER,
	CLIENT_WAIT_COMMAND,
	CLIENT_STREAMING,
	CLIENT_DISCONNECT
};

/* key-value pair metadata, passed as JSON output from receiver */
struct metadata {
	char*				key;
	char*				value;
	struct metadata 		*next;
};

/* individual static columns, passed output from receiver */
struct columndata {
	char				*value;
	struct columndata		*next;
};

struct client {
	int				sock;
	struct	sockaddr_storage 	addr;
	bool				compression,
					encryption,
					eof;
	size_t				chunklen,
					sequence;
	enum	client_state		state;
	struct	timespec 		ts;
	char				name[NI_MAXHOST],
					*prefix;

	struct	buffer 			*inbuffer; 
	struct	sendqueue		*outqueue;
	struct	metadata		*metadata;
	struct	columndata		*columndata;
	struct	client 			*next;
};

enum server_state {
	SERVER_CONNECTING,
	SERVER_PAUSED,
	SERVER_WAIT_ACK_STREAM,
	SERVER_STREAM
};

struct server {
	int				sock;
	struct	sockaddr_storage	addr;
	bool				compression,
					encryption,
					eof;
	size_t				sequence;
	enum	server_state		state;
	struct	timespec		ts;
	char				name[NI_MAXHOST];

	struct	buffer			*inbuffer;
	struct	sendqueue		sendqueue;
	struct	metadata		*metadata;
	struct	server			*next;
};

struct sendstats {
	size_t	bytesread,
		bytessent,
		chunkssent,
		discards;
};

bool iplist_load() {
	struct wordlist	*oldlist = g_options.iplist,
			*newlist = NULL;
	if( !g_options.opt_ip_file )
		return false;
	newlist = wordlist_from_file( g_options.opt_ip_file );
	if( ! newlist ) {
		errorf( "Failed to load IP whitelist: %s", g_options.opt_ip_file );
		return false;
	}
	infof( "Loaded %lu entries from IP whitelist: %s", newlist->ptr_size, g_options.opt_ip_file );
	wordlist_print( newlist );
	g_options.iplist = newlist;
	if( oldlist )
		wordlist_destroy( oldlist );
	return true;
}

void sendstats_tick(struct sendstats *stats) {
	stats->bytesread = 0;
	stats->bytessent = 0;
	stats->chunkssent = 0;
	/* discards is a running count */
}

struct metadata *metadata_new(struct metadata **mdlist, const char* key, const char *value) {
	struct metadata **p, *pnew;

	pnew = malloc( sizeof( struct metadata) );
	pnew->key = strdup( key );
	pnew->value = strdup( value );
	pnew->next = NULL;

	for( p = mdlist; *p; p = &(*p)->next )
		;
	(*p) = pnew;
	return pnew;
}

struct metadata *metadata_parse_opt(struct metadata **mdlist) {
	struct metadata *ret=NULL;
	char* s, *k, *v, *save;
	
	s = strdup( optarg );
	k = strtok_r( s, "=", &save );
	v = strtok_r( NULL, "=", &save );

	if(k && v)
		ret = metadata_new( mdlist, k, v );
	free(s);
	return ret;
}

void metadata_free( struct metadata **p ) {
	struct metadata *del;
	while( *p ) {
		del = *p;
		free( del->key );
		free( del->value );
		*p = (*p)->next;
		free( del );
	}
}

struct columndata *columndata_new( struct columndata **columnlist, const char* value ) {
	struct columndata **p, *pnew;

	pnew = malloc( sizeof( struct columndata ) );
	pnew->value = strdup( value );
	pnew->next = NULL;

	for( p = columnlist; *p; p = &(*p)->next )
		;
	(*p) = pnew;
	return pnew;
}

void columndata_free( struct columndata **p ) {
	struct columndata *del;
	while( *p ) {
		del = *p;
		free( del->value );
		*p = (*p)->next;
		free( del );
	}
}

/* TODO: make this non-blocking */
void write_header( int fd, int compression, int encryption, size_t buffersize, struct metadata *md, struct columndata *cd ) {
	char buf[BUFSIZE], s[1024], *p;
	ssize_t ret;
	size_t remaining, sent = 0;

	snprintf( buf, BUFSIZE, "BCBS %i %i %i %lu\n", protocol_version, compression, encryption, buffersize );

	for( ; md ; md = md->next ) {
		snprintf( s, 1024, "METADATA %s %s\n", md->key, md->value );
		strncat( buf, s, BUFSIZE - strlen(buf) );
		fprintf( stderr, "METADATA %s %s\n", md->key, md->value );
	}

	for( ; cd ; cd = cd->next ) {
		snprintf(s, 1024, "COLUMN %s\n", cd->value );
		strncat( buf, s, BUFSIZE - strlen(buf) );
		fprintf( stderr, "COLUMN %s\n", cd->value );
	}

	p = buf;
	remaining = strlen( buf );
	while( remaining ) {
		if( -1 == ( ret = write( fd, p, remaining ) ) )
			debugf( "write_header: %s", strerror( errno ) );
		else {
			remaining -= ret;
			p += ret;
			sent += ret;
		}
	}
	debugf( "write_header: sent %lu bytes", sent );
}

/* copy all complete lines from one buf to another */
struct buffer *prep_for_send(struct buffer *a, struct buffer **destpool, bool use_compression) {

	char *p, *nl = 0;
	struct buffer *b;
	size_t size;

	/* find last newline */
	for( p = a->buf + a->len - 1 ; p >= a->buf ; p-- ) {
		if( *p == '\n' ) {
			nl = p;
			break;
		}
	}
	if (!nl) {
		debugf( "no newlines found" );
		return NULL;
	}

	/* copy to an output buffer */
	size = nl - a->buf + 1;
	if( use_compression ) {
		size_t destlen, bound;
		int ret;
		bound = compressBound( size );
		b = buffer_pool_acquire( destpool, bound );
		debugf( "compressBound: %lu, buffer size: %lu", bound, b->size );
		destlen = b->size;
		if( Z_OK != (ret = compress( b->buf , &destlen, a->buf, size ) ) ) {
			errorf( "compress: %s", zError( ret ) );
			abort(); /* TODO: remove aborts for production */
		}
		b->len = destlen;
		debugf( "prep_for_send: compressed bytes: %lu, buffer size: %lu", b->len, b->size );
	} else {
		b = buffer_pool_acquire( destpool, size );
		memcpy( b->buf, a->buf, size );
		b->len = size;
		debugf( "moved %lu bytes to send buffer", size );
	}
	
	/* move any remaining text to top of buffer */
	if( size < a->len ) {
		debugf( "leftovers: source %lu dest %lu", a->len, size );
		buffer_pop( a, size );
	} else {
		a->len = 0;
	}

	/* return the acquired destination buffer */
	return b;

}

int sender() {
	struct	server *server;
	struct	addrinfo req, 
		*res;
	fd_set	iset, oset;
	struct 	buffer *ipb;
	struct 	timeval tv;
	bool	eof = false;
	struct 	buffer		*bufpool = NULL; 
	struct	sendqueue	*sendqueue = NULL;
	struct	timespec	tv0, tv1, stats_tv0, stats_tv1;
	bool			usecompression = true,
				done = false;
	struct	sendstats	stats = { 0, 0, 0, 0 };
	const size_t SERVERBUF_SIZE = 65536;
	char	serverbuf[65536];

	server = malloc( sizeof( struct server ) );
	bzero( server, sizeof( *server ) );

	/* setup connection to our receiver */
	
	memset(&req, 0, sizeof req);
	req.ai_family = AF_UNSPEC;
	req.ai_socktype = SOCK_STREAM;

	getaddrinfo( g_options.host, g_options.port, &req, &res);
	/* TODO: error check this */

	server->sock = socket( res->ai_family, res->ai_socktype, res->ai_protocol );
	if( server->sock == -1 )
		errstop( "socket");

	if( -1 == connect( server->sock, res->ai_addr, res->ai_addrlen ) )
		errstop( "connect");

	freeaddrinfo(res);

	/* send header */
	write_header( server->sock, 1, 1, BUFSIZE, g_options.md, g_options.cd );

	/* non-blocking i/o setup */
	nonblock( STDIN_FILENO );
	nonblock( server->sock );

	ipb = buffer_pool_acquire( &bufpool, BUFSIZE );

	clock_gettime( CLOCK_REALTIME, &tv0 );
	clock_gettime( CLOCK_REALTIME, &stats_tv0 );

	/* main loop */
	while( 1 ) {
		int numfds;
		bool readysend = false;

		FD_ZERO( &iset );
		FD_SET( STDIN_FILENO, &iset );
		FD_SET( server->sock, &iset ); /* to check for EOF from server */

		FD_ZERO( &oset );
		if( send_queue_count( sendqueue ) )
			FD_SET( server->sock, &oset );

		tv.tv_sec = 0;
		tv.tv_usec = 20000;

		if( -1 == (numfds = select( server->sock + 1, &iset, &oset, 0, &tv ) ) ) {
			errstop("select");
		}
				
		/* input ready */
		if( numfds ) {
			if( !eof && FD_ISSET( STDIN_FILENO, &iset ) ) {
				if(ipb->len == ipb->size) {
					errorf( "current buffer full SHOULD NOT SEE THIS" );
				}
				stats.bytesread += buffer_pool_read( STDIN_FILENO, ipb, &eof );
			}

			/* not expecting anything from the server, so likely EOF */
			if( FD_ISSET( server->sock, &iset ) && ( 0 == read( server->sock, &serverbuf, SERVERBUF_SIZE ) ) ) {
				errorf( "EOF from server. (IP whitelist rejection?)" );
				exit( 1 );
			}
		}


		/* wait for one of 2 conditions to send data:
			1. current buffer is full
			2. timeout has expired
			3. EOF occurred */
		clock_gettime( CLOCK_REALTIME, &tv1 );
		if( ( (double)tv1.tv_sec + (double)tv1.tv_nsec / 1000000000.0f ) - ( (double)tv0.tv_sec + (double)tv0.tv_nsec / 1000000000.0f ) > 2 ) {
			debugf( "readysend: time expired" );
			readysend |= true;
		}
		if( ipb->len == ipb->size ) {
			debugf( "readysend: current input buffer is full" );
			readysend |= true;
		}
		if( eof ) {
			debugf( "readysend: EOF" );
			readysend |= true;
		}
		if( readysend ) {
			struct buffer *opb, *cmd;
			if( NULL != ( opb = prep_for_send( ipb, &bufpool, usecompression ) ) ) {
				/* send command */
				cmd = buffer_pool_acquire( &bufpool, 1024 );
				cmd->len = snprintf( cmd->buf, cmd->size, "CHUNK %lu %lu\n", ++server->sequence, opb->len );
				send_queue_add_buffer( &sendqueue, cmd );
				/* send stream */
				send_queue_add_buffer( &sendqueue, opb );
			}
			memcpy( &tv0, &tv1, sizeof( struct timespec ) );
		}

		/* output ready */
		if( numfds && FD_ISSET( server->sock, &oset ) ) {
			stats.bytessent += send_queue_transmit( server->sock, &sendqueue );
			stats.chunkssent++;
		}

		/* terminate conditions */
		done |= ( eof && !send_queue_count( sendqueue ) );

		/* memory management and stats */
		clock_gettime( CLOCK_REALTIME, &stats_tv1 );		
		if( stats_tv1.tv_sec - stats_tv0.tv_sec > 2 || done ) {
			time_t t;
			size_t qalloc = send_queue_allocated( sendqueue );

			time( &t );

			if( send_queue_allocated( sendqueue ) > g_options.max_memory ) {
				size_t overage = qalloc - g_options.max_memory;
				debugf("memory overage: %lu bytes, discarding oldest chunks", overage);
				stats.discards += send_queue_discard( &sendqueue, overage );
			}
			buffer_pool_gc_by_age( &bufpool, g_options.max_buffer_age );

			/* strftime( buf, 1024, "%s :: %Y-%m-%d %H:%M:%S", &lt ); */
			printf( "%lu :: read: %lu sent: %lu bufs: %lu bufsiz: %lu qlen: %lu qsiz: %lu qalloc: %lu discard: %lu\n", 
				t, stats.bytesread, stats.bytessent,
				buffer_pool_count( bufpool ), buffer_pool_size( bufpool ),
				send_queue_count( sendqueue ), send_queue_size( sendqueue ),
				qalloc, stats.discards );
			fflush( stdout );
			sendstats_tick( &stats );
			memcpy( &stats_tv0, &stats_tv1, sizeof( stats_tv0 ) );
		}

		/* terminate */
		if( done )
			break;

	}

	buffer_pool_release( ipb );
	buffer_pool_gc( &bufpool );
	if( g_options.md )
		metadata_free( &g_options.md );

	if( -1 == close( server->sock ) )
		errstop( "close" );

	free( server );
	
	return 0;
}

char *build_prefix_string( char **deststr, struct metadata *md, struct columndata *cd ) {
	char buf[BUFSIZE];
	char *cur = buf, *end = cur + sizeof( buf );

	cur = buf;
	for( ; cd ; cd = cd->next ) { 
		debugf("found columndata: [%s]", cd->value );
		cur += snprintf( cur, (size_t) ( end - cur ), "%s ", cd->value );
	}
	cur += snprintf( cur, (size_t) ( end - cur ), "\"metadata\":{" );
	for( ; md ; md = md->next ) {
		debugf("found metadata: [%s] --> [%s]", md->key, md->value);
		cur += snprintf( cur, (size_t) ( end - cur ), "\"%s\":\"%s\"%s", md->key, md->value, md->next ? "," : "" );
	}
	cur += snprintf( cur, (size_t) ( end - cur ), "} " );
	*deststr = strdup( buf );
	debugf("built metadata prefix string: %s", *deststr);
	return *deststr;
}

int client_accept(int sock, struct client **clientpool, struct buffer **bufferpool) {
	int count = 0;
	while( 1 ) {
		int newsock;
		struct client **pc, *pnew;
		struct sockaddr_storage addr;
		socklen_t addr_size = sizeof( addr );
		char name[NI_MAXHOST];

		newsock = accept( sock, (struct sockaddr *) &addr, &addr_size );
		if( newsock == -1 ) {
			if( errno != EWOULDBLOCK && errno != EAGAIN )
				errorf( "accept: %s", strerror( errno ) );
			break;
		}

		getnameinfo(
			(struct sockaddr *)&addr, 
			sizeof( addr ), 
			(char *) &name, sizeof(name), 
			NULL, 0, 
			NI_NUMERICHOST);

		if( g_options.opt_ip_file && ! wordlist_match_any( g_options.iplist, name ) ) {
			close( newsock );
			infof( "Rejected connection from %s: not in IP whitelist.", name );
			continue;
		}

		pnew = malloc( sizeof( struct client ) );
		bzero( pnew, sizeof( struct client ) );
		pnew->sock = newsock;
		memcpy( &(pnew->addr), &addr, sizeof( addr ) );
		pnew->compression = false;
		pnew->eof = false;
		pnew->state = CLIENT_WAIT_HEADER;
		clock_gettime( CLOCK_REALTIME, &(pnew->ts) );
		strcpy( pnew->name, name );
		metadata_new( &pnew->metadata, "source_addr", pnew->name );
		pnew->inbuffer = buffer_pool_acquire( bufferpool, BUFSIZE );
		pnew->outqueue = NULL;
		pnew->next = NULL;

		nonblock( newsock );

		for( pc = clientpool; *pc; pc = &(*pc)->next )
			;
		*pc = pnew;

		infof( "accept: new connection from %s", pnew->name );
	}
	return count;
}

void client_disconnects( struct client **pool ) {
	struct client **p, *del;

	p = pool;
	while( *p ) {
		if( (*p)->state == CLIENT_DISCONNECT && !(*p)->outqueue ) {
			del = *p;
			infof( "closing client %s", del->name );
			close( del->sock );
			if( del->inbuffer )
				buffer_pool_release( del->inbuffer );
			if( del->prefix )
				free( del->prefix );
			metadata_free( &del->metadata );
			columndata_free( &del->columndata );
			*p = (*p)->next;
			free( del );
		} else
			p = &(*p)->next;
	}
}

bool client_check_header( struct client *pc, struct buffer **bufpool ) {
	const char *delims = " \t\r\n";
	char *s = NULL, *n = NULL, *line = NULL, *bcbs = NULL, *saveptr = NULL;
	int version = 0, compression = 0, encryption = 0;
	size_t buffersize = 0;

	debugf( "client_check_header" );
	if( NULL == ( line = buffer_getline( pc->inbuffer ) ) )
		return false;

	/* header: bcbs */
	bcbs = strtok_r( line, delims, &saveptr );
	if( !bcbs || strcmp( bcbs, "BCBS" ) ) {
		errorf( "Invalid header BCBS (%s) from %s", bcbs, pc->name );
		pc->state = CLIENT_DISCONNECT;
		free( line );
		return false;
	}
	
	/* header: version */
	errno = 0;
	s = strtok_r( NULL, delims, &saveptr );
	if(s)
		version = strtol( s, &n, 10 );
	if( !s || n == s || version != protocol_version ) {
		errorf( "Invalid header version (%s) from %s", s ? s : "null", pc->name );
		pc->state = CLIENT_DISCONNECT;
	}
	
	/* header: compression */
	errno = 0;
	s = strtok_r( NULL, delims, &saveptr );
	if(s)
		compression = strtol( s, &n, 10 );
	if( !s || n == s ) {
		errorf( "Invalid header compression (%s) from %s", s ? s : "null", pc->name );
		pc->state = CLIENT_DISCONNECT;
	}
	pc->compression = compression;

	/* header: encryption */
	errno = 0;
	s = strtok_r( NULL, delims, &saveptr );
	if(s)
		encryption = strtol( s, &n, 10 );
	if( !s || n == s ) {
		errorf( "Invalid header encryption (%s) from %s", s ? s : "null", pc->name );
		pc->state = CLIENT_DISCONNECT;
	}
	pc->encryption = encryption;

	/* header: buffer size */
	errno = 0;
	s = strtok_r( NULL, delims, &saveptr );
	if(s)
		buffersize = strtol( s, &n, 10 );
	if( !s || n == s ) {
		errorf( "Invalid header buffer size (%s) from %s", s ? s : "null", pc->name );
		pc->state = CLIENT_DISCONNECT;
	}

	infof( "Client %s requesting a buffer size of %lu %s compression", pc->name, buffersize, pc->compression ? "with" : "without" );
	if( pc->inbuffer->size < buffersize ) {
		size_t oldsize = pc->inbuffer->size;
		buffer_pool_release( pc->inbuffer );
		pc->inbuffer = buffer_pool_acquire( bufpool, buffersize );
		infof( "Client %s resized buffer from %lu to %lu", pc->name, oldsize, buffersize );
		/* TODO: broken, because we may have already started receiving the stream. */
	}
	pc->state = CLIENT_WAIT_COMMAND;
	free( line );
	return true;
}

void client_write( struct client *pc, const char *str, struct buffer **bufpool )
{
	struct buffer *buf;

	buf = buffer_pool_acquire( bufpool, strlen( str ) );
	send_queue_add_buffer( &pc->outqueue, buf );
}

bool client_read_stream( struct client* pc, struct buffer **bufferpool )
{
	debugf("client_read_stream");
	if( pc->compression ) {
		size_t expected, remaining, destlen;
		ssize_t ret;
		char *buf;
		struct buffer *buffer;

		/* if we don't have all the data we want, hang out for a bit */
		debugf( "buffer pool length: %lu", pc->inbuffer->len );

		/* expected includes size_t header */
		expected = pc->chunklen;
		if(expected > pc->inbuffer->size) {
			errorf( "client_read_stream: expected > inbuffer->size.  shouldn't see this." );
			abort(); /* TODO: remove for production */
		}
		if( pc->inbuffer->len < expected ) {
			debugf( "streaming: not enough to read a full chunk: %lu", expected );
			return false;
		}

		debugf( "streaming: have enough to decompress a chunk: expected: %lu, buffer: %lu", expected, pc->inbuffer->len );
		buffer = buffer_pool_acquire( bufferpool, pc->inbuffer->size );

		destlen = buffer->size;
		if( Z_OK != ( ret = uncompress( buffer->buf, &destlen, pc->inbuffer->buf, expected ) ) ) {
			errorf( "uncompress: %s", zError( ret ) );
			abort(); /* TODO: remove for production */
		}
		buffer->len = destlen;

		/* line-by-line write prepended with provided metadata in json */
		if( g_options.prefix ) {
			char *line;
			while( ( line = buffer_getline( buffer ) ) ) {
				printf( "%s%s", pc->prefix, line ); /* TODO: no printf */
				free( line );
			}
			fflush( stdout );
		}
		/* fast block writes */
		else {
			remaining = buffer->len;
			buf = buffer->buf;

			while( remaining ) {
				if( -1 == ( ret = write( STDOUT_FILENO, buf, remaining ) ) )
					errorf( "write: %s", strerror( errno ) );
				else {
					buf += ret;
					remaining -= ret;
				}
			}
		}
		/* adjust input buffer for leftovers, buffer start should be a size_t header */
		debugf( "expected: %lu, len: %lu", expected, pc->inbuffer->len );
		buffer_pop( pc->inbuffer, expected );
		buffer_pool_release( buffer );

	} else {
		/* TODO: handle uncompressed case */
	}

	debugf("CLIENT_WAIT_COMMAND");
	pc->state = CLIENT_WAIT_COMMAND;
	return true;
}

void client_check_command( struct client *pc, struct buffer **bufpool ) {
	const char *delims = " \t\r\n";
	char *s = NULL, *n = NULL, *line = NULL, *saveptr = NULL, *k, *v;

	debugf("client_check_command");
	while( ( line = buffer_getline( pc->inbuffer ) ) ) {
		
		s = strtok_r( line, delims, &saveptr );

		/* COLUMN */
		if( !strcmp(s, "COLUMN") ) {
			v = strtok_r( NULL, delims, &saveptr );
			debugf( "Added column: [%s]", v );
			columndata_new( &pc->columndata, v );
			build_prefix_string( &pc->prefix, pc->metadata, pc->columndata );
		}

		/* METADATA */
		else if( !strcmp(s, "METADATA") ) {
			k = strtok_r( NULL, delims, &saveptr );
			v = strtok_r( NULL, delims, &saveptr );
			debugf( "Added metadata: [%s] --> [%s]", k, v );
			metadata_new( &pc->metadata, k, v );
			if( pc->prefix )
				free( pc->prefix );
			build_prefix_string( &pc->prefix, pc->metadata, pc->columndata );
		} 

		/* CHUNK */
		else if ( !strcmp( s, "CHUNK" ) ) {

			/* sequence */
			errno = 0;
			s = strtok_r( NULL, delims, &saveptr );
			if(s)
				pc->sequence = strtol( s, &n, 10 );
			if( !s || n == s ) {
				errorf( "client_check_command: invalid sequence: %s", s );
				client_write( pc, "ERROR CHUNK SEQUENCE", bufpool );
				free( line );
				continue;
			}

			/* chunk size */
			errno = 0;
			s = strtok_r( NULL, delims, &saveptr );
			if(s)
				pc->chunklen = strtol( s, &n, 10 );
			if( !s || n == s ) {
				errorf( "client_check_command: invalid chunk size: %s", s );
				client_write( pc, "ERROR CHUNK SIZE", bufpool );
				free( line );
				continue;
			}

			pc->state = CLIENT_STREAMING;
			debugf("CLIENT_STREAMING");
			if( !client_read_stream( pc, bufpool ) ) {
				free( line );
				return;
			}
		}
		
		/* INVALID */
		else {
			client_write( pc, "ERROR INVALID COMMAND", bufpool );
		}
		free( line );
	}
}

void receiverhandler(int signo) {
	switch( signo ) {
		case SIGPIPE:
			errorf( "Received SIGPIPE. Exiting..." );
			g_options.fl_sigint = true;
			break;
		case SIGINT:
			infof( "Received SIGINT. Exiting..." );
			g_options.fl_sigint = true;
			break;
		case SIGUSR1:
			infof( "Received SIGUSR1. Reloading config files..." );
			g_options.fl_reload = true;
			break;
		default:
			errorf( "Received unexpected signal: %i", signo );
	};
}

int receiver() {
	int sock, ret, yes = 1;
	struct addrinfo hints, *res;
	struct client *clientpool = NULL;
	struct buffer *bufferpool = NULL;

	memset( &hints, 0, sizeof hints );
	hints.ai_family = AF_INET;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_flags = AI_PASSIVE;

	if( SIG_ERR ==  signal( SIGPIPE, receiverhandler ) )
		errstop( "signal SIGPIPE" );
	if( SIG_ERR ==  signal( SIGINT, receiverhandler ) )
		errstop( "signal SIGINT" );
	if( SIG_ERR ==  signal( SIGUSR1, receiverhandler ) )
		errstop( "signal SIGUSR1" );
	if( SIG_ERR ==  signal( SIGUSR2, receiverhandler ) )
		errstop( "signal SIGUSR2" );

	if( 0 != (ret = getaddrinfo(NULL, g_options.port, &hints, &res) ) ) {
		errorf( "getaddrinfo: %s", strerror( ret ) );
		exit( 1 );
	}

	sock = socket( res->ai_family, res->ai_socktype, res->ai_protocol );
	if( sock == -1 )
		errstop( "socket");

	if( -1 == setsockopt( sock, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof( int ) ) ) 
		errstop( "setsockopt" );

	if ( -1 == bind( sock, res->ai_addr, res->ai_addrlen ) )
		errstop( "bind" );
	freeaddrinfo( res );

	if( -1 == listen( sock, 10 ) ) {
		errstop( "listen");
	}

	nonblock( sock );

	/* main loop */
	infof( "Receiver started listening on port %s", g_options.port );
	debugf( "entering main loop" );
	while ( 1 ) {
		struct timespec ts;
		int nfds = 0, numready;
		struct 	timeval tv;
		struct client *pc;
		fd_set	iset;
		
		clock_gettime( CLOCK_REALTIME, &ts );

		tv.tv_sec = 0;
		tv.tv_usec = 50000;

		FD_ZERO( &iset );
		FD_SET( sock, &iset );
		
		nfds = sock + 1;
		for( pc = clientpool; pc; pc = pc->next ) {
			if( !pc->eof )
				FD_SET( pc->sock, &iset );
			if( pc->sock >= nfds )
				nfds = pc->sock + 1;
		}

		if( -1 == ( numready = select( nfds, &iset, 0, 0, &tv ) ) ) {
			errorf( "select: %s", strerror( errno ) );
		}

		/* TODO: NOTE! This numready condition is only if new I/O has happened */
		if( numready ) {
			debugf( "number ready: %i", numready );

			/* accept new server connections */
			if( FD_ISSET( sock, &iset ) ) 
				client_accept( sock, &clientpool, &bufferpool );

			/* attempt reads from all ready clients */
			for( pc = clientpool; pc; pc = pc->next ) {
				bool fdready = FD_ISSET( pc->sock, &iset );
				if( fdready )
					if( 0 == buffer_pool_read( pc->sock, pc->inbuffer, &pc->eof ) ) {
						infof( "Received EOF from client: %s", pc->name );
						pc->state = CLIENT_DISCONNECT;
					}
				if(!fdready && !pc->inbuffer->len)
					continue;
				/* check for header */
				if( pc->state == CLIENT_WAIT_HEADER ) 
					client_check_header( pc, &bufferpool );
				/* check for command */
				if( pc->state == CLIENT_WAIT_COMMAND )
					client_check_command( pc, &bufferpool );
				/* stream reading */
				if( pc->state == CLIENT_STREAMING ) {
					while( client_read_stream( pc, &bufferpool ) && pc->state == CLIENT_STREAMING )
						;
				}
			}
			

			/* TODO: check for eof on sockets */

		} /* if( numready ) */

		/* check for timeouts */
		for( pc = clientpool; pc; pc = pc->next ) {
			if( pc->state == CLIENT_WAIT_HEADER && ts.tv_sec - pc->ts.tv_sec > 10 ) {
				errorf( "Client %s timed out", pc->name );
				pc->state = CLIENT_DISCONNECT;
			}
		}

		/* TODO: send any output (won't have much for this application */

		/* clean up disconnects */
		client_disconnects( &clientpool );

		/* handle signal flags */
		if( g_options.fl_sigint ) {
			/* TODO: cleanup */
			infof( "Caught SIGINT, exiting..." );
			break;
		}
		if( g_options.fl_reload ) {
			if( g_options.opt_ip_file ) { 
				if( iplist_load() ) {
					infof( "IP list reload successful." );
				} else {
					errorf( "IP list reload failed. ");
				}
			}
			g_options.fl_reload = false;
		}

	} /* while( 1 ) */

	buffer_pool_gc( &bufferpool );

	if( g_options.iplist )
		wordlist_destroy( g_options.iplist );

	return 0;
}

int main(int argc, char *argv[]) {
	int c, i;

	/* default options */
	bzero( &g_options, sizeof( g_options ) );
	g_options.max_buffer_age = 300;
	g_options.max_memory = 2147483648; /* 2 gigs default */

	while( -1 != ( c = getopt( argc, argv, "ldm:c:pt:e:i:v" ) ) )
		switch( c ) {
		case 'l':
			g_options.listen = true;
			break;
		case 'd':
			g_common_options.debug = true;
			break;
		case 'm':
			if(!metadata_parse_opt( &g_options.md )) {
				fprintf(stderr, "Invalid metadata: format: -mkey=value\n");
				return 1;
			}
			break;
		case 'c':
			columndata_new( &g_options.cd, optarg );
		case 'p':
			debugf("column and metadata prefix enabled");
			g_options.prefix = true;
			break;
		case 't':
			g_options.max_buffer_age = atol( optarg );
			if( ! g_options.max_buffer_age ) {
				fprintf( stderr, "Invalid max_buffer_age: %li\n", g_options.max_buffer_age );
				return 1;
			}
			fprintf( stderr, "Max buffer age: %li seconds\n", g_options.max_buffer_age );
			break;
		case 'e':
			g_options.max_memory = atol( optarg );
			if( ! g_options.max_memory ) {
				fprintf( stderr, "Invalid max_memory: %li\n", g_options.max_memory );
				return 1;
			}
			fprintf( stderr, "Max buffer age: %li seconds\n", g_options.max_memory );
			break;
		case 'i':
			g_options.opt_ip_file = optarg;
			if( ! iplist_load() ) 
				exit( 1 );
			break;
		case '?':
			/* TODO: check options with arguments */
			if(isprint ( optopt) ) {
				fprintf( stderr, "Unknown option '%c'.\n", optopt );
				return 1;
			} else {
				fprintf( stderr, "Unknown option character '\\x%x'.\n", optopt );
				return 1;
			}
			break;
		case 'v':
			fprintf( stderr, "%s (%s)\n", __FILE__, g_commit );
			exit( 1 );
			break;
		default:
			abort();
		}

	debugf( "getopt: optind: %d, argc: %d", optind, argc );
	i = optind;

	if( g_options.listen ) {
		if( argc - optind < 1 ) {
			fprintf( stderr, "port required\n" );
			return 1;
		}
		g_options.port = argv[i++];
	} else {
		if( argc - optind  < 2 ) {
			fprintf( stderr, "host and port required\n" );
			return 1;
		}
		g_options.host = argv[i++];
		g_options.port = argv[i++];
	}

	if ( g_options.listen  ) {
		debugf( "configured as receiver" );
		return receiver();
	} else {
		debugf( "configured as sender" );
		return sender();
	}
}
