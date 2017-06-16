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
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <errno.h>
#include <sys/time.h>
#ifdef __MACH__
#include <mach/clock.h>
#include <mach/mach.h>
#endif
#ifndef NOGEOIP
#include <GeoIPCity.h>
#endif
#include "common.h"

#define BUFSIZE 8192
#define OBUFSIZE 16384
#define GBUFSIZE 64
#define MAXFIELDS 128

#define IS_SPLIT(C) ( in_curly ? 0 : ((C)==' '|| (C)=='\n'||(C)=='\t'||(C)==0 || (in_quotes || n_fields-1==27 || n_fields-1==28 ? 0 : ((C)==':'||(C)=='['||(C)==']'||(C)=='/'))))

/* enum for fields indexes - these align with the tokenized input string and the field info in the table below */
typedef enum field { 
	SYSLOG_MONTH,
	SYSLOG_DAY,
	SYSLOG_HOUR,
	SYSLOG_MINUTE,
	SYSLOG_SECOND,
	SYSLOG_SERVER,
	PROGRAM,
	PID,
	CLIENT_IP,
	CLIENT_PORT,
	HAPROXY_MONTHDAY,
	HAPROXY_MONTH,
	HAPROXY_YEAR,
	HAPROXY_HOUR,
	HAPROXY_MINUTE,
	HAPROXY_SECOND,
	HAPROXY_MILLISECONDS,
	FRONTEND_NAME,
	BACKEND_NAME,
	SERVER_NAME,
	TIME_REQUEST,
	TIME_QUEUE,
	TIME_BACKEND_CONNECT,
	TIME_BACKEND_RESPONSE,
	TIME_DURATION,
	HTTP_STATUS_CODE,
	BYTES_READ,
	CAPTURED_REQUEST_COOKIE,
	CAPTURED_RESPONSE_COOKIE,
	TERMINATION_STATE,
	ACTCONN,
	FECONN,
	BECONN,
	SRVCONN,
	RETRIES,
	SRV_QUEUE,
	BACKEND_QUEUE,
	CAPTURED_REQUEST_HEADERS,
	HTTP_VERB,
	HTTP_REQUEST,
	HTTP_VERSION,
	MESSAGE,		/* output only */
	SYSLOG_TIMESTAMP, 	/* output only */
	HAPROXY_TIME, 		/* output only */
	VERSION, 		/* output only */
	TIMESTAMP, 		/* output only */
	REQUEST_PATHS,		/* output only */
	REQUEST_VAR,		/* output only */
	TIME_DATA,		/* output only */
	GEOIP, 			/* output only (geoip) */
	LOCATION,		/* output only (geoip) */
	LATITUDE, 		/* output only (geoip) */
	LONGITUDE, 		/* output only (geoip) */
	REAL_REGION_NAME, 	/* output only (geoip) */
	TIMEZONE, 		/* output only (geoip) */
	IP, 			/* output only (geoip) */
	COUNTRY_CODE2, 		/* output only (geoip) */
	COUNTRY_CODE3, 		/* output only (geoip) */
	COUNTRY_NAME, 		/* output only (geoip) */
	CONTINENT_CODE, 	/* output only (geoip) */
	REGION_NAME, 		/* output only (geoip) */
	CITY_NAME, 		/* output only (geoip) */
	COUNT,			/* output only (debug) */
	SSL_ERROR,		/* output only (ssl) */
	REQUESTVAR_KEY,		/* output only (request var) */
	REQUESTVAR_VALUE,	/* output only (request var) */
	REDISPATCH,		/* output only (retries starts with +) */
	FRONTEND_NAMES,		/* output only (split frontend names) */
	TERMINATION_STATES,	/* output only (split termination states) */
	SSL,			/* output only (ssl flag) */
	VER,			/* output only (consumer version */
	TRACK			/* output only (tracking cookie: set, got, ref, mut */
} Field;

typedef struct {
	const char* name;
	const int numeric,
		  lean; /* level of leanness to reject */
} FieldInfo;

const char g_commit[] = COMMIT;

/* json output fields */
const FieldInfo fieldinfo[] = 
{
	/* name                         numeric	lean */
	{ "syslog_month", 		0,	0 },
	{ "syslog_day", 		0,	0 },
	{ "syslog_hour", 		0,	0 },
	{ "syslog_minute", 		0,	0 },
	{ "syslog_second", 		0,	0 },
	{ "syslog_server", 		0,	0 },
	{ "program", 			0,	0 },
	{ "pid", 			0,	0 },
	{ "client_ip", 			0,	9 },
	{ "client_port", 		0,	9 },
	{ "haproxy_monthday", 		0,	0 },
	{ "haproxy_month", 		0,	0 },
	{ "haproxy_year", 		0,	0 },
	{ "haproxy_hour", 		0,	0 },
	{ "haproxy_minute", 		0,	0 },
	{ "haproxy_second", 		0,	0 },
	{ "haproxy_milliseconds", 	0,	0 },
	{ "frontend_name", 		0,	9 },
	{ "backend_name", 		0,	9 },
	{ "server_name", 		0,	9 },
	{ "time_request", 		1,	9 },
	{ "time_queue", 		1,	9 },
	{ "time_backend_connect", 	1,	9 },
	{ "time_backend_response", 	1,	9 },
	{ "time_duration", 		1,	9 },
	{ "http_status_code", 		0,	9 },
	{ "bytes_read", 		1,	9 },
	{ "captured_request_cookie", 	0,	9 },
	{ "captured_response_cookie", 	0,	9 },
	{ "termination_state", 		0,	9 },
	{ "actconn", 			1,	9 },
	{ "feconn", 			1,	9 },
	{ "beconn", 			1,	9 },
	{ "srvconn", 			1,	9 },
	{ "retries", 			1,	9 },
	{ "srv_queue", 			1,	9 },
	{ "backend_queue", 		1,	9 },
	{ "captured_request_headers", 	0,	9 },
	{ "http_verb", 			0,	9 },
	{ "http_request", 		0,	9 },
	{ "http_version", 		0,	9 },
	{ "message", 			0,	9 }, /* output only */
	{ "syslog_timestamp", 		0,	0 }, /* output only */
	{ "haproxy_time", 		0,	0 }, /* output only */
	{ "@version", 			0,	9 }, /* output only */
	{ "@timestamp", 		0,	9 }, /* output only */
	{ "request_paths",		0,	9 }, /* output only */
	{ "request_var",		0,	9 }, /* output only */
	{ "time_data",			0,	9 }, /* output only */
	{ "geoip",			0,	9 }, /* output only */
	{ "location",			0,	9 }, /* output only */
	{ "latitude",			0,	9 }, /* output only (geoip) */
	{ "longitude",			0,	9 }, /* output only (geoip) */
	{ "real_region_name",		0,	9 }, /* output only (geoip) */
	{ "timezone",			0,	9 }, /* output only (geoip) */
	{ "ip",				0,	0 }, /* output only (geoip) */
	{ "country_code2",		0,	9 }, /* output only (geoip) */
	{ "country_code3",		0,	9 }, /* output only (geoip) */
	{ "country_name",		0,	9 }, /* output only (geoip) */
	{ "continent_code",		0,	9 }, /* output only (geoip) */
	{ "region_name",		0,	9 }, /* output only (geoip) */
	{ "city_name", 			0,	9 }, /* output only (geoip) */
	{ "count",			0,	9 }, /* output only (debug) */
	{ "ssl_error",			0,	9 }, /* output only (ssl) */
	{ "key",			0,	9 }, /* output only (request var) */
	{ "value",			0,	9 }, /* output only (request var) */
	{ "redispatch",			0,	9 }, /* output only (retries starts with +) */
	{ "frontend_names",		0,	9 }, /* output only (split frontend names) */
	{ "termination_states",		0,	9 }, /* output only (split termination states) */
	{ "ssl",			0,	9 }, /* output only (ssl flag) */
	{ "ver",			0,	9 }, /* output only (consumer version */
	{ "track",			0,	9 }, /* output only (tracking cookie: set, got, ref, mut */
	{ NULL, 			0,	0 }
};

/* buffers */
char buf[BUFSIZE], message[BUFSIZE]; /* working log line and a copy */
char obuf[BUFSIZE*4]; /* output buffer */
int o; /* position tracking inside the output buffer: obuf[o] */

int	fields[MAXFIELDS];	/* index into buf for each field */

static inline void output_start_record() {
	obuf[o++] = '{';
}

static inline void output_end_record() {
	obuf[o++] = '}';
}

static inline void output_start_array() {
	obuf[o++] = '[';
}

static inline void output_end_array() {
	obuf[o++] = ']';
}

static inline void output_field_sep() {
	obuf[o++] = ',';
}

static inline void output_field_name(int f) {
	int j;

	obuf[o++]='"';
	for(j=0;fieldinfo[f].name[j]!=0;++j)
		obuf[o++]=fieldinfo[f].name[j];
	obuf[o++]='"';
	obuf[o++]=':';
}

static inline void output_quoted_field(int f) {
	int j;

	obuf[o++]='"'; \
	for(j=0;buf[fields[f]+j]!=0;++j)
		obuf[o++]=buf[fields[f]+j];
	obuf[o++]='"';
}

static inline void output_quoted_field_escape(int f) {
	int j;

	obuf[o++]='"';
	for(j=0;buf[fields[f]+j]!=0;++j) {
		if(buf[fields[f]+j]=='\\')
			obuf[o++] = '\\';
		obuf[o++]=buf[fields[f]+j];
	}
	obuf[o++]='"';
}

static inline void output_unquoted_field(int f) {
	int j;

	for(j=0;buf[fields[f]+j]!=0;++j)
		obuf[o++]=buf[fields[f]+j];
}


static inline void output_quoted_var( const char *s ) {
	int j;

	obuf[o++]='"';
	for(j=0;s[j]!=0;++j)
		obuf[o++]=s[j];
	obuf[o++]='"';
}

static inline void output_quoted_var_escape( const char *s ) {
	int j;

	obuf[o++]='"';
	for(j=0;s[j]!=0;++j) {
		if(s[j]=='\\')
			obuf[o++]='\\';
		obuf[o++]=s[j];
	}
	obuf[o++]='"';
}

static inline void output_unquoted_var( const char *s ) {
	int j;

	for(j=0;s[j]!=0;++j) {
		obuf[o++]=s[j];
	}
}

static inline void output_space() {
	obuf[o++]=' ';
}

static inline void output_colon() {
	obuf[o++]=':';
}

static inline void output_quote() {
	obuf[o++]='"';
}

static inline void output_haproxy_month() {
	switch( buf[fields[HAPROXY_MONTH]] ) {
		case 'A':
			switch( buf[fields[HAPROXY_MONTH]+1] ) {
				case 'p': obuf[o++]='0'; obuf[o++]='4'; break;
				case 'u': obuf[o++]='0'; obuf[o++]='8'; break;
				default: fprintf(stderr, "HAPROXY_MONTH: A%c, %s\n", buf[fields[HAPROXY_MONTH]+1], message); abort();
			};
			break;
		case 'D': obuf[o++]='1'; obuf[o++]='2'; break;
		case 'F': obuf[o++]='0'; obuf[o++]='2'; break;
		case 'J':
			switch( buf[fields[HAPROXY_MONTH]+1] ) {
				case 'a': obuf[o++]='0'; obuf[o++]='1'; break;
				case 'u':
					switch( buf[fields[HAPROXY_MONTH]+2] ) {
						case 'n': obuf[o++]='0'; obuf[o++]='6'; break;
						case 'l': obuf[o++]='0'; obuf[o++]='7'; break;
						default: fprintf(stderr, "HAPROXY_MONTH: Ju%c, %s\n", buf[fields[HAPROXY_MONTH]+2], message); abort();
					};
					break;
			};
			break;
		case 'M':
			switch( buf[fields[HAPROXY_MONTH]+2] ) {
				case 'r': obuf[o++]='0'; obuf[o++]='3'; break;
				case 'y': obuf[o++]='0'; obuf[o++]='5'; break;
				default: fprintf(stderr, "HAPROXY_MONTH: Ma%c, %s\n", buf[fields[HAPROXY_MONTH]+2], message); abort();
			}
			break;
		case 'N': obuf[o++]='1'; obuf[o++]='1'; break;
		case 'O': obuf[o++]='1'; obuf[o++]='0'; break;
		case 'S': obuf[o++]='0'; obuf[o++]='9'; break;
		default: fprintf(stderr, "HAPROXY_MONTH: %c, %s\n", buf[fields[HAPROXY_MONTH]], message); abort();
	};
}

int tnum( const char *s, long int *i ) {
	char *endptr;
	errno = 0;
	*i = strtol( s, &endptr, 10 );
	if( errno )
		return 0;
	if( *endptr != '\0' )
		return 0;
	return 1;
}

#ifndef NOGEOIP
static const char *_mk_NA(const char *p)
{   
	return p ? p : "N/A";
}
#endif

void platformclock( struct timespec *ts )
{
#ifdef __MACH__ /* OS X does not have clock_gettime, use clock_get_time */
	clock_serv_t cclock;
	mach_timespec_t mts;
	host_get_clock_service(mach_host_self(), CALENDAR_CLOCK, &cclock);
	clock_get_time(cclock, &mts);
	mach_port_deallocate(mach_task_self(), cclock);
	ts->tv_sec = mts.tv_sec;
	ts->tv_nsec = mts.tv_nsec;
#else
	clock_gettime(CLOCK_REALTIME, ts);
#endif
}

int main( int argc, char **argv )
{
#ifndef NOGEOIP
	/* geoip vars */
	char *env_geoip_path=NULL;
	GeoIP *gi=0;
	GeoIPRecord *gir=0;
	char **gret=0;
	const char *gtz=0;
#endif

	/* line counting */
	long count=0;

	/* performance */
	struct timespec start_time, end_time;
	double seconds;

	/* options */
	int 	opt_col_count=0,	/* print out column counts only (debug) */
		opt_long_lines_only=0,	/* only output lines with more columns than expected */
		opt_short_lines_only=0, /* only output lines with fewer columns than expected */
		opt_sequence_num=0,     /* add count field to each outputted json document with the input line # */
		opt_bad_lines=0,	/* output skipped, ssl error, or badreq lines to stderr */
		opt_geoip=1,		/* geoip lookups (default unless NOGEOIP defined */
		opt_dissect_request=1,	/* dissect request into its component paths and http variables */
		opt_echo_line=0,	/* echo each input line before outputting json */
		opt_prefix=0,		/* log lines are prepended with a json object containing source metadata */
		opt_lean=0,		/* weight to reject field from emitting output - make output leaner */
		opt_timestamp=0,	/* prefix with timestamp so bulkloader can use to create index name */
		opt_servername=0,	/* prefix with servername so bulkloader can use to create index name */
		opt_derived_time=0,	/* calculate Td (data transmission time) = Tt - (Tq + Tw + Tc + Tr) */
		opt_split_termstate=0,	/* split termination state into individual components */
		opt_cookie_compare=0;	/* analyze tracking cookie for sets, got, ref, and mut */
	char   *opt_split_frontend=NULL;	/* split frontend_name with specified character */
	char   *opt_vars_file=NULL;
	struct wordlist *wl=NULL;
	char c;

	while ((c = getopt (argc, argv, "nlshbgrepf:w:tmdvF:Tc")) != -1)
		switch (c) {
			case 'n':
				opt_col_count = 1;
				break;
			case 'l':
				opt_long_lines_only = 1;
				break;
			case 's':
				opt_sequence_num = 1;
				break;
			case 'h':
				opt_short_lines_only = 1;
				break;
			case 'b':
				opt_bad_lines = 1;
				break;
			case 'g':
				opt_geoip=0;
				break;
			case 'r':
				opt_dissect_request=0;
				break;
			case 'e':
				opt_echo_line=1;
				break;
			case 'p':
				opt_prefix=1;
				break;
			case 'f':
				opt_vars_file = optarg;
				break;
			case 'w':
				opt_lean = atoi( optarg );
				break;
			case 't':
				opt_timestamp=1;
				break;
			case 'm':
				opt_servername=1;
				break;
			case 'd':
				opt_derived_time=1;
				break;
			case 'v':
				fprintf( stderr, "%s (%s)\n", __FILE__, g_commit );
				exit( 1 );
				break;
			case 'F':
				opt_split_frontend=optarg;
				break;
			case 'T':
				opt_split_termstate=1;
				break;
			case 'c':
				opt_cookie_compare=1;
				break;
		};
	      
	if(opt_vars_file) {
		wl = wordlist_from_file( opt_vars_file );
		if( NULL == wl ) {
			errorf( "Failed to load wordlist: %s", opt_vars_file );
			exit( 1 );
		}
	}

#ifndef NOGEOIP
	if(opt_geoip) {
		/* geoip options:
			GEOIP_STANDARD = 0, 	Read database from file system. This uses the least memory.
		    	GEOIP_MEMORY_CACHE = 1, Load database into memory. Provides faster performance but uses more memory.
		        GEOIP_CHECK_CACHE = 2,  Check for updated database. If database has been updated, reload file handle and/or memory cache.
			GEOIP_INDEX_CACHE = 4,  Cache only the the most frequently accessed index portion of the database, resulting in faster 
						lookups than GEOIP_STANDARD, but less memory usage than GEOIP_MEMORY_CACHE. This is useful for 
						larger databases such as GeoIP Organization and GeoIP City. Note: for GeoIP Country, Region and 
						Netspeed databases, GEOIP_INDEX_CACHE is equivalent to GEOIP_MEMORY_CACHE.
			GEOIP_MMAP_CACHE = 8 	Load database into mmap shared memory. MMAP is not available for 32bit Windows. */
		env_geoip_path = getenv("GEOIPDB");
		if( env_geoip_path ) {
			gi = GeoIP_open(env_geoip_path, GEOIP_MEMORY_CACHE);
		} else {
			gi = GeoIP_open("geoip/GeoLiteCity.dat", GEOIP_MEMORY_CACHE);
		}
		if (gi == NULL) {
		fprintf(stderr, "GeoIP_open: Error opening database. Set GEOIPDB environment variable to full path of your GeoIP City Lite database\n"
				"            or use -g to disable GeoIP lookups\n");
			exit(1);
		}
	}
#endif

	/* main loop - line by line */
	platformclock ( &start_time );
	while(NULL != fgets( buf, BUFSIZE, stdin)) {
		int	n_fields=1, 		/* field count */
			i=0, 			/* counter i (used in macros) */
			j=0, 			/* counter j (used in macros) */
			length=strlen(buf), 	/* log line length */
			in_quotes=0, 		/* boolean - tracking inside quotes "" */
			in_curly = 0,		/* boolean - tracking inside curlies {} */
			fl_sslerr = 0,		/* boolean - true if ssl error */
			fl_redispatch = 0;	/* boolean - redispatch (retries has + prefix) */
			char *prefix = NULL,	/* metadata from prefix */
			     *index = NULL,	/* index from prefix */
			     *doctype = NULL,	/* doctype from prefix */
			     *saveptr;		/* saveptr for strtok_r */
			size_t prefixskip = 0;	/* bytes to skip to get past prefix and NULL terminator */
		
		/* reset output buffer */	
		o=0;
		obuf[0]='\0';

		/* echo input line */
		if( opt_echo_line ) {
			fputs( "\n", stderr );
			fputs( buf, stderr );
			fflush( stderr );
		}

		/* grab prefix */
		if( opt_prefix ) {
			index = strtok_r( buf, " ", &saveptr );
			doctype = strtok_r( NULL, " ", &saveptr );
			prefix = strtok_r( NULL, " ", &saveptr );
			prefixskip = saveptr - buf;
		}
			
		/* assuming first field starts on first character */
		++count;
		fields[0]=prefixskip; 

		/* copy message field - escape quotes */
		i=prefixskip; 
		j=0;
		while(j<BUFSIZE-1&&buf[i]!='\n'&&buf[i]!=0) {
			if(buf[i]=='"' || buf[i]=='\\') 
				message[j++] = '\\';
			message[j++]=buf[i++];
		}
		message[j]='\0';

		/* in-place tokenize */
		for( i = prefixskip + 1; i < length; ++i ) {
			/* change delimiter set when inside quotes or curly braces */
			if(buf[i] == '"') {
				in_quotes = !in_quotes;
				buf[i] = ' ';
			}
			else if( ( n_fields != HTTP_REQUEST + 1 ) && ( buf[i] == '{' ) ) {
			/* use curly braces as delimeter except for HTTP request field  */
				in_curly = 1;
				fields[n_fields++] = i;
			}
			else if(buf[i-1] == '}')
				in_curly = 0;
			/* Replace first non-content character following the end of a field with a null to terminate the string */
			if(!IS_SPLIT(buf[i-1]) && IS_SPLIT(buf[i]))
				buf[i]=0;
			/* First character after whitespace or delimiter is a new field, record in fields */
			else if(IS_SPLIT(buf[i-1]) && !IS_SPLIT(buf[i])) {
				fields[n_fields++] = i;
				/* hack - remove period between seconds.milliseconds */
				if(n_fields == HAPROXY_SECOND+1)
					buf[i+2] = ' ';
				/* ssl event: BACKEND_NAME == 1, don't break up remaining line, SERVER_NAME contains error */
				else if(n_fields == BACKEND_NAME+2 && buf[fields[BACKEND_NAME]]=='1') {
					while(buf[i]!='\n'&&buf[i]!=0)
						++i;
					buf[i]=0;
					fl_sslerr=1;
					break;
				}
			}
		}

		/* debug option: output only column count */
		if( opt_col_count ) {
			printf("%i\n", n_fields);
			continue;
		}
		
		/* debug option: output only unexpectedly long lines */
		if( opt_long_lines_only && n_fields <= 41 )
			continue;

		/* debug option: output only unexpectedly short lines */
		if( opt_short_lines_only && n_fields >= 40 )
			continue;

		/* skip lines that aren't an event */
		if( buf[fields[CLIENT_IP]] < '0' || buf[fields[CLIENT_IP]] > '9' ) {
			if( opt_bad_lines )
				fprintf(stderr, "%s\n", message);
			continue;
		}

		/* for tracking ssl event - not skipped, but will show with opt_bad_lines */
		if( opt_bad_lines && fl_sslerr)
			fprintf(stderr, "%s\n", message);

		/* for tracking <BADREQ> - not skipped, but show with opt_bad_lines */
		if( opt_bad_lines && buf[fields[HTTP_VERB]] == '<')
			fprintf(stderr, "%s\n", message);

		/* opt_servername */
		if( opt_servername ) {
			output_unquoted_field( SERVER_NAME );
			obuf[o++] = '\t';
		}

		/* opt_timestamp */
		if( opt_timestamp ) {
			output_unquoted_field(HAPROXY_YEAR);
			obuf[o++] = '.';
			output_haproxy_month();
			obuf[o++]='.';
			output_unquoted_field(HAPROXY_MONTHDAY);
			obuf[o++]='.';
			output_unquoted_field(HAPROXY_HOUR);
			obuf[o++] = '\t';
		}

		/* output prefix index and doctype */
		if( opt_prefix ) {
			output_unquoted_var( index );
			obuf[o++] = '\t';
			output_unquoted_var( doctype );
			obuf[o++] = '\t';
		}

		/* output new json document */
		output_start_record();

		/* prefix metadata */
		if( opt_prefix ) {
			output_unquoted_var(prefix);
			output_field_sep();
		}

		/* original message */
		output_field_name(MESSAGE);
		output_quoted_var(message);

		/* output version */
		output_field_sep();
		output_field_name(VER);
		output_quoted_var( g_commit );

		/* line number - debug option */
		if ( opt_sequence_num ) {
			output_field_sep();
			output_field_name(COUNT);
			o+=sprintf( obuf, "%li", count );
		}

		if( !opt_lean ) {
			/* add syslog_timestamp */
			output_field_sep();
			output_field_name(SYSLOG_TIMESTAMP);
			output_quote();
			output_unquoted_field(SYSLOG_MONTH);
			output_space();
			output_unquoted_field(SYSLOG_DAY);
			output_space();
			output_unquoted_field(SYSLOG_HOUR);
			output_colon();
			output_unquoted_field(SYSLOG_MINUTE);
			output_colon();
			output_unquoted_field(SYSLOG_SECOND);
			output_quote();

			/* Add haproxy_time */
			output_field_sep();
			output_field_name(HAPROXY_TIME);
			output_quote();
			output_unquoted_field(HAPROXY_HOUR);
			output_colon();
			output_unquoted_field(HAPROXY_MINUTE);
			output_colon();
			output_unquoted_field(HAPROXY_SECOND);
			output_quote();
		}

		/* Add @version */
		output_field_sep();
		output_field_name(VERSION);
		output_quote();
		obuf[o++] = '1'; /* TODO: make this an command line option */
		output_quote();

		/* @timestamp */
		output_field_sep();
		output_field_name(TIMESTAMP);
		output_quote();

		output_unquoted_field(HAPROXY_YEAR);
		obuf[o++] = '-';
		output_haproxy_month();
		obuf[o++]='-';
		output_unquoted_field(HAPROXY_MONTHDAY);
		obuf[o++]='T';
		output_unquoted_field(HAPROXY_HOUR);
		obuf[o++]=':';
		output_unquoted_field(HAPROXY_MINUTE);
		obuf[o++]=':';
		output_unquoted_field(HAPROXY_SECOND);
		obuf[o++]='.';
		output_unquoted_field(HAPROXY_MILLISECONDS);
		obuf[o++]='Z';
		output_quote();

		/* check for + prefix on retries field, remove and set flag */
		if( buf[fields[RETRIES]] == '+' ) {
			fl_redispatch = 1;
			fields[RETRIES]++;
		}

		/* redispatch flag */
		output_field_sep();
		output_field_name(REDISPATCH);
		output_unquoted_var( fl_redispatch ? "true" : "false" );

		/* split termination state */
		if( opt_split_termstate && strlen( &buf[fields[TERMINATION_STATE]] ) == 4 ) {
			int i;
			output_field_sep();
			output_field_name( TERMINATION_STATES );
			output_start_record();
			for( i=0; i<4; ++i ) {
				if(i)
					output_field_sep();
				output_quote();
				o+=sprintf( &obuf[o], "%d", i );
				output_quote();
				output_colon();
				output_quote();
				obuf[o++]=buf[fields[TERMINATION_STATE]+i];
				output_quote();
			}
			output_end_record();
		}

		/* output all one-to-one mapped fields */
		for( i=0; i<n_fields; ++i) {
			/* skip fields beyond a certain weight */
			if(opt_lean > fieldinfo[i].lean)
				continue;
			output_field_sep();
			output_field_name(i);
			if(fieldinfo[i].numeric)  /* macros contain multiple statements */ 
				output_unquoted_field(i);
			else if(i == HTTP_REQUEST)
				output_quoted_field_escape(i);
			else
				output_quoted_field(i);
		}

		/* remove trailing ~ on ssl frontend_name */
		{
			int len;
			char *s;
			output_field_sep();
			output_field_name(SSL);
			s = &buf[ fields[ FRONTEND_NAME ] ];
			len = strlen( s );
			/* fprintf( stderr, "[%s] [%d] [%c]\n", s, len, s[len-1] ); */
			if( s[len-1] == '~' ) {
				s[len-1] = '\0';
				output_unquoted_var("true");
			} else {
				output_unquoted_var("false");
			}
		}

		/* split frontend name */
		if( opt_split_frontend ) {
			int n = 0;
			char 	*p = NULL,
				*saveptr = NULL, 
				*a = &buf[ fields[ FRONTEND_NAME ] ];
			output_field_sep();
			output_field_name( FRONTEND_NAMES );
			output_start_record();
			while( NULL != ( p = strtok_r( n ? NULL : a, opt_split_frontend, &saveptr ) ) ) {
				if( n )
					output_field_sep();
				output_quote();
				o+=sprintf( &obuf[o], "%d", n );
				output_quote();
				output_colon();
				output_quoted_var( p );
				n++;
			}
			output_end_record();
		}

		/* dissect request, don't attempt to dissect a <BADREQ> */
		if( opt_dissect_request && !fl_sslerr && buf[fields[HTTP_VERB]] != '<') {
			int	n = 0, /* number of fields read in */
				j = 0; /* number of fields writen out */
			char	*a = &buf[ fields[ HTTP_REQUEST ] ],
				*p;
			char *saveptr, *saveptr_inner = 0, *question;
			
			question = strchr( a, '?' );
			if( question )
				*question = '\0';
			output_field_sep();
			output_field_name( REQUEST_PATHS );
			output_start_record();
			while( NULL != ( p = strtok_r( n ? NULL : a, "/", &saveptr ) ) ) {
				if( n )
					output_field_sep();
				output_quote();
				o+=sprintf( &obuf[o], "%02d", n );
				output_quote();
				output_colon();
				output_quoted_var( p );
				n++;
			}
			output_end_record();

			output_field_sep();
			output_field_name( REQUEST_VAR );
			output_start_record();
			if(question) {
				n = j = 0;
				p = question + 1;
				while( NULL != ( p = strtok_r( n ? NULL : p, "&", &saveptr ) ) ) {
					char *key = NULL, *value = NULL;
					key = strtok_r( p, "=", &saveptr_inner );
					if( ( key && opt_vars_file && wordlist_match_any( wl, key ) ) || ( key && !opt_vars_file ) ) {
						value = strtok_r( NULL, "=", &saveptr_inner );
						if(j)
							output_field_sep();
						output_quoted_var_escape( key );
						output_colon();
						if( value ) {
							output_quoted_var_escape( value );
						} else {
							obuf[o++] = '"';
							obuf[o++] = '"';
						}
						j++;
					}
					n++;
				}
			}
			output_end_record();
		}

#ifndef NOGEOIP
		if(opt_geoip) {
			/* GeoIP Lookup */
			gir = GeoIP_record_by_name(gi, (const char *)&buf[fields[CLIENT_IP]]);
			if( gir != NULL) {
				gret = GeoIP_range_by_ip(gi, (const char *)&buf[fields[CLIENT_IP]]);
				gtz = GeoIP_time_zone_by_country_and_region(gir->country_code, gir->region);

				output_field_sep();
				output_field_name(GEOIP);
				output_start_record();
					output_field_name(LOCATION);
					output_start_array();
						o+=sprintf( &obuf[o], "%f", gir->longitude );
						output_field_sep();
						o+=sprintf( &obuf[o], "%f", gir->latitude );
					output_end_array();
					output_field_sep();
					
					output_field_name(REAL_REGION_NAME);
					output_quoted_var(_mk_NA(GeoIP_region_name_by_code (gir->country_code, gir->region)));
					output_field_sep();

					output_field_name(TIMEZONE);
					output_quoted_var(_mk_NA(gtz));
					output_field_sep();

					output_field_name(IP);
					output_quoted_field(CLIENT_IP);
					output_field_sep();

					output_field_name(COUNTRY_CODE2);
					output_quoted_var(_mk_NA(gir->country_code));
					output_field_sep();

					output_field_name(COUNTRY_CODE3);
					output_quoted_var(_mk_NA(gir->country_code3));
					output_field_sep();

					output_field_name(COUNTRY_NAME);
					output_quoted_var(_mk_NA(gir->country_name));
					output_field_sep();

					output_field_name(CONTINENT_CODE);
					output_quoted_var(_mk_NA(gir->continent_code));
					output_field_sep();

					output_field_name(REGION_NAME);
					output_quoted_var(_mk_NA(gir->region));
					output_field_sep();

					output_field_name(CITY_NAME);
					output_quoted_var(_mk_NA(gir->city));
					output_field_sep();

					output_field_name(LATITUDE);
					output_quote();
					o+=sprintf( &obuf[o], "%f", gir->latitude );
					output_quote();
					output_field_sep();

					output_field_name(LONGITUDE);
					output_quote();
					o+=sprintf( &obuf[o], "%f", gir->longitude );
					output_quote();

				output_end_record();

				GeoIP_range_by_ip_delete(gret);
				GeoIPRecord_delete(gir);
			}
		} /* if(opt_geoip) */

		if( opt_derived_time ) {
			long int td, tt, tq, tw, tc, tr;
			if( tnum ( &buf[ fields[ TIME_REQUEST ] ], &tq) && tq != -1 &&
			    tnum ( &buf[ fields[ TIME_QUEUE ] ],   &tw) && tw != -1 &&
			    tnum ( &buf[ fields[ TIME_BACKEND_CONNECT ] ], &tc) && tc != -1 &&
			    tnum ( &buf[ fields[ TIME_BACKEND_RESPONSE ] ], &tr) && tr != -1 &&
			    tnum ( &buf[ fields[ TIME_DURATION ] ], &tt) && tr != -1 ) {
					output_field_sep();
					output_field_name( TIME_DATA );
					td = tt - ( tq + tw + tc + tr );
					o += sprintf( &obuf[o], "%li", td );
			}
		}

		if( opt_cookie_compare ) {
			output_field_sep();
			output_field_name(TRACK);
			if( ( buf[fields[CAPTURED_REQUEST_COOKIE]] == '-' ) && ( buf[fields[CAPTURED_RESPONSE_COOKIE]] == '-' ) )
				output_quoted_var( "neg" );
			else if( ( buf[fields[CAPTURED_REQUEST_COOKIE]] != '-' ) && ( buf[fields[CAPTURED_RESPONSE_COOKIE]] == '-' ) )
				output_quoted_var( "got" );
			else if( ( buf[fields[CAPTURED_REQUEST_COOKIE]] == '-' ) && ( buf[fields[CAPTURED_RESPONSE_COOKIE]] != '-' ) )
				output_quoted_var( "set" );
			else if( ( buf[fields[CAPTURED_REQUEST_COOKIE]] != '-' ) && ( buf[fields[CAPTURED_RESPONSE_COOKIE]] != '-' ) ) {
				if( strcmp( &buf[fields[CAPTURED_REQUEST_COOKIE]], &buf[fields[CAPTURED_RESPONSE_COOKIE]] ) == 0 )
					output_quoted_var( "ref" );
				else
					output_quoted_var( "mut" );
			} else
				output_quoted_var( "err" );
		}
#endif		
		output_end_record();

		/* null terminate and write */
		obuf[o++]=0;
		puts(obuf);
	}
	
	/* performance */
	platformclock ( &end_time );
	seconds = ( (double) end_time.tv_sec + ((double) end_time.tv_nsec / (double) 1000000000) ) - 
		  ( (double) start_time.tv_sec + ((double) start_time.tv_nsec / (double) 1000000000) );
	fprintf(stderr, "Processed %li lines in %.2f seconds. (%.2f lines/sec).\n", count, seconds, (double)count / seconds);

#ifndef NOGEOIP
	if(opt_geoip)
		GeoIP_delete(gi);
#endif
	return 0;
}
