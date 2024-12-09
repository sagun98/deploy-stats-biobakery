#!/usr/bin/env python

from __future__ import print_function

import os
import sys
import re

import argparse
import bz2
import collections
import csv
import gzip
import random
import shutil
import subprocess
import textwrap
import time

# ---------------------------------------------------------------
# progress tracker with times
# ---------------------------------------------------------------

class Progress( ):
    
    def __init__( self, total, update=None, prefix="PROGRESS", timescale="s" ):
        # easy properties
        self.total     = total
        self.prefix    = prefix
        self.time      = time.time( )
        self.time0     = self.time
        self.counter   = 0
        self.duration  = None
        self.timescale = timescale
        # determine how often to update
        update = 0.05 if update is None else update
        # **** max 1 needed to prevent update < 1 for small total ****
        self.update = max( 1, int( update * total ) ) if update < 1 else update

    def tick( self ):
        self.counter += 1
        if self.counter >= self.update:
            # **** force a report for the last cycle ****
            if self.counter % self.update == 0 or self.counter == self.total:
                self.report( )

    def report( self ):
        # percent progress
        fields = []
        format = {"PREFIX": self.prefix}
        frac = self.counter / float( self.total )
        # format the total
        format["TOTAL"] = "{:,}".format( self.total )
        # format the counter
        total_field = len( format["TOTAL"] )
        format["COUNT"] = "{A: >{B},}".format( A=self.counter, B=total_field )
        # format the percent
        format["PERC"] = "{: >5.1f}%".format( 100 * frac )
        # determine time and other time math
        format["SCALE"] = self.timescale
        norm = {"s":1, "m":60, "h":3600}[self.timescale]
        new_time  = time.time( )
        delta     = (new_time - self.time) / norm
        self.time = new_time
        elapsed   = (self.time - self.time0) / norm
        remains   = 0 if frac == 1 else (elapsed / frac - elapsed)
        # used to estimate the width of the time field
        if self.duration is None:
            self.duration = "{:.1f}".format( elapsed / frac / norm )
        time_field = 1 + max( 2, len( self.duration ) )
        # format the time fields
        format["DELTA"]   = "{A: >{B}.1f}".format( A=delta,   B=time_field )
        format["ELAPSED"] = "{A: >{B}.1f}".format( A=elapsed, B=time_field )
        format["REMAINS"] = "{A: >{B}.1f}".format( A=remains, B=time_field )
        # put the message together
        message = [
            "{PREFIX}: {COUNT} of {TOTAL} ({PERC})",
            "D {DELTA}{SCALE}",
            "E {ELAPSED}{SCALE}",
            "R {REMAINS}{SCALE}",
        ]
        say( " | ".join( message ).format( **format ) )

# ---------------------------------------------------------------
# warnings
# ---------------------------------------------------------------

def say( *args, **kwargs ):
    """ shortcut for printing to stderr """
    termination = kwargs.get( "end", "\n" )
    destination = kwargs.get( "file", sys.stderr )
    print( " ".join( map( str, args ) ), end=termination, file=destination )

def die( *args, **kwargs ):
    """ say a messange and then exit """
    args = ["DIED FROM ERROR:"] + list( args )
    say( *args, **kwargs )
    sys.exit( )

def confirm( message="DANGEROUS OPERATION!", wait=5 ):
    """ prompt user to type digits to confirm an operation, then do a countdown """
    say( message )
    say( "-" * len( message ) )
    digits = "{:06d}".format( int( 1e6 * random.random( ) ) )
    answer = raw_input( "To confirm, please type these digits <{}>: ".format( digits ) ).strip( ) 
    if answer != digits:
        die( "Your answer did not match." )
    elif wait > 0:
        span = len( str( wait ) )
        while wait > 0:
            say( "Executing in {A:{B}d} seconds".format( A=wait, B=span ) )
            wait -= 1
            time.sleep( 1 )
    say( "Executing!" )
    return True

# deprecate: probably used by scripts/modules
def warn( *args ):
    script = "?"
    if sys.argv[0] != "":
        script = os.path.split( sys.argv[0] )[1].upper()
    args = ["WARNING ({}):".format( script )] + list( args )
    print( " ".join( map( str, args ) ), file=sys.stderr )

# ---------------------------------------------------------------
# shortcut: defining lists and dicts
# ---------------------------------------------------------------

def qw( multiline_string, as_dict=False, delim="\s+", comment="#" ):
    """ inspired by Perl's qw function """
    ret = [k for k in multiline_string.split( "\n" ) \
        if k != "" and k[0] != comment]
    if as_dict:
        d = collections.OrderedDict( )
        for line in ret:
            items = re.split( delim, line )
            if len( items ) != 2:
                die( "bad qw as_dict line:", line )
            else:
                d[items[0]] = items[1]
        ret = d
    return ret

# ---------------------------------------------------------------
# shortcut: defining a simple command-line interface
# ---------------------------------------------------------------

"""
FORMATTING EXAMPLES
-------------------
arg               : simple arg
--arg             : simple flag arg
--arg|float       : specifying a type
--arg|float|3.14  : specifying a type and a default value
--arg|bool        : special case to handle <action="store_true">
--arg|path        : a path argument (str with better metavar)
"""

def args( config ):
    parser = argparse.ArgumentParser( )
    argfuncs = {
        "int":   int,
        "float": float,
        "path":  str,
        "str":   str,
    }
    for line in qw( config ):
        # defaults
        arg, argtype, argdef = None, "str", None
        # what kind of specification is this?
        items = re.split( "\|", line.strip( ) )
        # plain argument
        if len( items ) == 1:
            arg = items[0]
        # argument with type
        elif len( items ) == 2:
            arg, argtype = items
        # argument with type and default value
        elif len( items ) == 3:
            arg, argtype, argdef = items
        # special case for flags
        if argtype == "bool":
            parser.add_argument( arg, action="store_true", help="flag" )            
        # default case
        else:
            argfunc = argfuncs[argtype]
            argdef  = None if argdef is None else argfunc( argdef )
            parser.add_argument( 
                arg, 
                type=argfunc,
                default=argdef,
                metavar="<{}>".format( argtype ),
                help="default: {}".format( argdef ),
            )
    return parser.parse_args( )

# ---------------------------------------------------------------
# path interaction
# ---------------------------------------------------------------

def try_open( path, mode="r", *args, **kwargs ):
    """ open a (possibly compressed?) file or fail gracefully """
    fh = None
    try:
        # 1) load as gzip file
        if path.endswith( ".gz" ):
            say( "Treating", path, "as gzip file" )
            # python 2/3 switching
            if sys.version_info.major == 3:
                opener = gzip.open
                mode = "rt" if mode == "r" else mode
            else:
                opener = gzip.GzipFile
            fh = opener( path, mode=mode, *args, **kwargs )
        # 2) load as bz2 file
        elif path.endswith( ".bz2" ):
            say( "Treating", path, "as bzip2 file" )
            # python 2/3 switching
            if sys.version_info.major == 3:
                opener = bz2.open
                mode = "rt" if mode == "r" else mode
            else:
                opener = bz2.BZ2File
            fh = opener( path, mode=mode, *args, **kwargs )
        # 3) load as regular file
        else:
            fh = open( path, mode=mode, *args, **kwargs )
    except:
        die( "Problem opening", path )
    return fh

def which( program ):
    """
    Adapted from:
    https://stackoverflow.com/questions/377017/test-if-executable-exists-in-python
    """
    ret = None
    def is_exe( fpath ):
        return os.path.isfile( fpath ) and os.access( fpath, os.X_OK )
    fpath, fname = os.path.split( program )
    if fpath and is_exe( program ):
        ret = program
    else:
        for path in os.environ["PATH"].split( os.pathsep ):
            path = path.strip( '"' )
            exe_file = os.path.join( path, program )
            if is_exe( exe_file ):
                ret = exe_file
    return ret

# ---------------------------------------------------------------
# safely move a file (rename until unique)
# ---------------------------------------------------------------

def safe_move( old_path, new_dir, verbose=True ):
    # safety checks
    if os.path.isdir( old_path ):
        die( "safe_move source <{}> is a directory".format( old_path ) )
    if not os.path.isdir( new_dir ):
        die( "safe_move target <{}> is not a directory".format( new_dir ) )
    # build candidate new path
    old_dir, file_name = os.path.split( old_path )
    new_path = os.path.join( new_dir, file_name )
    # check for uniqueness
    counter = 1
    while os.path.isfile( new_path ):
        counter += 1
        # extract name, then stem + extension
        file_name = os.path.split( new_path )[1]
        stem, extension = os.path.splitext( file_name )
        # 1st iteration after collision
        if counter == 2 and verbose:
            say( "moving:", old_path )
            say( "  ", "exists:", new_path )
        # 2nd iteration (or later): remove previous suffix
        else:
            stem = re.sub( "_[0-9]{3}$", "", stem )
        # attach suffix to file name
        file_name = "{}_{:03d}{}".format( stem, counter, extension )
        # build new path
        new_path = os.path.join( new_dir, file_name )
        # update
        if verbose:
            say( "  ", "trying:", new_path )
    # attempt to actually move now that new_path is unique
    shutil.move( old_path, new_path )

# ---------------------------------------------------------------
# methods for reading from files / stdout
# ---------------------------------------------------------------

def reader( fh, dialect="excel-tab" ):
    """ my favorite options for csv reader """
    for row in csv.reader( fh, dialect ):
        yield row

class LineCounter( ):
    """ helper class for counting processed lines """
    def __init__( self, path, verbose ):
        self.path    = path
        self.verbose = verbose
        self.counter = 0
    def tick( self ):
        self.counter += 1
        if self.verbose and self.counter % ( 1e5 ) == 0:
            say( "<{}> lines processed (millions): {:.1f}".format( 
                self.path, self.counter / 1e6 ) )

def iter_lines( path, skip=0, verbose=True ):
    """ easy file loading """
    LC = LineCounter( path, verbose )
    with try_open( path ) as fh:
        for line in fh:
            LC.tick( )
            if LC.counter > skip:
                yield line.rstrip( )

def iter_rows( path, skip=0, verbose=True, dialect="excel-tab" ):
    """ easy table loading """
    lens = collections.Counter( )
    LC = LineCounter( path, verbose )
    with try_open( path ) as fh:
        for row in reader( fh, dialect=dialect ):
            LC.tick( )
            if LC.counter > skip:
                lens[len( row )] += 1
                yield row
    if len( lens ) != 1 and verbose:
        say( "non-uniform row lengths:", sorted( lens, key=lambda x: -lens[x] ) )

def iter_rowdicts( path, verbose=True, dialect="excel-tab" ):
    """ easy table loading """
    headers = None
    for row in iter_rows( path, skip=0, verbose=verbose, dialect=dialect ):
        if headers is None:
            headers = row
        elif len( row ) == len( headers ):
            yield {k:v for k, v in zip( headers, row )}
        else:
            say( "can't align headers to row:", row )

def iter_stdout( command, encoding="utf-8", verbose=True  ):
    """ recipe for iterable capture of process stdout """
    LC = LineCounter( "STDOUT", verbose )
    if type( command ) is not list:
        die( "<iter_stdout> requires command list: e.g. ['cat', 'file.txt']" )
    stream = subprocess.Popen( command, stdout=subprocess.PIPE )
    while True:
        line = stream.stdout.readline( )
        if not line:
            break
        LC.tick( )
        # the lines come out as bytestrings: e.g. b'hello'
        yield line.decode( encoding ).rstrip( )

# ---------------------------------------------------------------
# methods for writing data
# ---------------------------------------------------------------

def tprint( *args, **kwargs ):
    """ coerce list of items to strings then print with tabs between """
    # this is a workaround for Python 2 (can't have "file=None" follow *args)
    file = kwargs.get( "file", sys.stdout )
    print( "\t".join( [str( k ) for k in args] ), file=file )

def write_rowdict( headers, values=None, file=None ):
    file = sys.stdout if file is None else file
    # mode for headers
    if values is None:
        tprint( *headers, file=file )
    # mode for data row
    else:
        row = []
        for k in headers:
            if k not in values:
                die( "Bad rowdict: couldn't find:", k, "in", values.keys( ) )
            row.append( values[k] )
        tprint( *row, file=file )

# ---------------------------------------------------------------
# text manipulation
# ---------------------------------------------------------------

def cleave( text, delim, index=1 ):
    """ cleave( "sample.output.txt", ".", -1 ) returns "sample.output" and "txt" """
    items = text.split( delim )
    a = delim.join( items[:index] )
    b = delim.join( items[index:] )
    return a, b

def smartwrap( text, charlim ):
    """ this function was already available in python... """
    return textwrap.fill( text, width=charlim )

def path2name( path ):
    """ given '/blah/blah/blah/hello.txt.gz' returns 'hello'"""
    return os.path.split( path )[1].split( "." )[0]

def rebase( path, newext=None, newdir=None ):
    olddir, name = os.path.split( path )
    if newext is not None:
        name = name.split( "." )[0:-1]
        name = ".".join( name + [newext] )
    newdir = newdir if newdir is not None else olddir
    return os.path.join( newdir, name )

def col2list( filename, index=0, limit=None, func=None, headers=False ):
    """ quickly load a column from a file """
    ret = []
    counter = 0
    for row in iter_rows( filename ):
        if headers:
            headers = False
        else:
            ret.append( row[index] )
            counter += 1
        if limit is not None and counter >= limit:
            break
    if func is not None:
        ret = [func( k ) for k in ret]
    return ret

def shorten( string, n=10, dummy="[...]" ):
    t = 2 * n + len( dummy )
    if len( string ) >= t:
        string = string[0:n] + dummy + string[-n:]
    return string

# ---------------------------------------------------------------
# dictionary methods (move to dictation?)
# ---------------------------------------------------------------

# move to dictation
def sorteditems( d, reverse=False, limit=None ):
    """ return k, v pairs in v-sorted order """
    counter = 0
    for key in sorted( d.keys( ), key=lambda x: d[x], reverse=reverse ):
        if limit is None or counter < limit:
            yield key, d[key]
            counter += 1
        else:
            break

# deprecate: in use
def sortedby( iterable, sorter, reverse=False ):
    if len( iterable ) != len( sorter ):
        die( "sortedby on iterables of non-equal lengths" )
    pairs = [[s, i] for s, i in zip( sorter, iterable )]
    pairs.sort( reverse=reverse )
    return [pair[1] for pair in pairs]

# deprecate
def autodict( iDepth=None, funcDefault=None ):
    """ 
    Acts as a constructor; makes an avdict 
    Can terminate at a specified depth as a defaultdict with the specified constructor.
    Example1: x = funcAVD( 3, int ) allows x["foo"]["bar"]["net"] += 1 ( i.e., a counter )
    Example2: x = funcAVD( 2, list ) allows x["foo"]["bar"].append( "net" ) 
    """
    if iDepth is None:
        return collections.defaultdict( lambda: autodict( None ) )
    elif iDepth >= 2:
        return collections.defaultdict( lambda: autodict( iDepth - 1, funcDefault ) )
    elif iDepth == 1:
        return collections.defaultdict( funcDefault )

# ---------------------------------------------------------------
# tests
# ---------------------------------------------------------------

if __name__ == "__main__":
    pass
