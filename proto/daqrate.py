#!/usr/bin/env python

import sys
import os

USAGE="\
   Usage: %s [options] <num dect and src> <num sink> <qsize> <run#>\n\
Examples: %s 5 5\n\
          %s 4 4 --nodes=grunt{1-5} --ddnodes=1 # 1 node  sending to 4\n\
          %s 4 4 --nodes=grunt{1-5} --ddnodes=4 # 4 nodes sending to 1\n\
          %s 5 5 --nodes=grunt{1-5} --ddnodes=0 # \n\
          %s 5 5 --nodes=grunt{1-5} # same as above\
"%((os.path.basename(sys.argv[0]),)*6)




###############################################################################

g_opt={'tlvlmsk':0}
if sys.version_info[0] == 2: exec( 'trc_one=1L' )
else:                        exec( 'trc_one=1' )


def TRACE( lvl, fmt_s, *args ):
    import socket,time,os
    global g_thisnode
    # how is g_opt working w/o "global" declaration? - must default if read
    if g_opt['tlvlmsk'] & (trc_one<<lvl):
        if g_thisnode == None: g_thisnode=NodeInfo()
        fo = open( "%s.%s.trc"%(g_thisnode.hostnames_l[0],os.getenv('RGANG_MACH_ID')), "a+" )
        fd = fo.fileno()
        os.write( fd,'%.2f:%s:%s:%d:%s\n'%(time.time(),socket.gethostname(),
                                           g_thisnode.mach_idx,lvl,fmt_s%args))
        fo.close()
    # TRACE


###############################################################################
# General Regular Expression class that allows for:
#    xx = Re( re )
#    ...
#    if xx.search( line ):
#        yy = xx.match_obj.group( x )
#

class Re:
    import re
    def __init__( self, reg_ex=None,flags=0 ):
        if reg_ex: self.compiled = self.re.compile(reg_ex,flags)
        self.match_obj=None
    def search(self,arg1,string=None):
        if string: self.match_obj = self.re.search(arg1, string)
        else:      self.match_obj = self.compiled.search( arg1 )
        return self.match_obj
    # Re

re_numeric = Re( r"([0-9a-f]+)-((0x{0,1}){0,1}[0-9a-f]+)" )     # the "r" prefix --> use Python's raw string notation
re_1alpha  = Re( r"([a-zA-Z])-([a-zA-Z])" )                     # the "r" prefix --> use Python's raw string notation
re_hex    = Re( r"^[0-9a-f]+" )



def findall_expands( ss ):
    result = []; result_idx = 0; brace_lvl = 0
    for cc in ss:
        if   cc == '{':
            brace_lvl = brace_lvl + 1
            if brace_lvl == 1: result.append('')
        if brace_lvl > 0: result[result_idx] = result[result_idx] + cc
        if   cc == '}':
            brace_lvl = brace_lvl - 1
            if brace_lvl == 0: result_idx = result_idx + 1
    if brace_lvl != 0: result.pop()
    return result
    # findall_expands


def numeric_expand( ss_l ):
    ret = []
    for sss in ss_l:
        # single alpha check 1st so {a-f} is not mistaken for
        # integer (not hex) numeric expand
        if re_1alpha.search( sss ):
            start = re_1alpha.match_obj.group(1)
            end   = re_1alpha.match_obj.group(2)
            end   = chr(ord(end)+1)
            while start != end:
                ret.append( start )
                start = chr(ord(start)+1)
        elif re_numeric.search( sss ):
            start = re_numeric.match_obj.group(1)
            end   = re_numeric.match_obj.group(2)
            bb    = re_numeric.match_obj.group(3)
            if   bb == None:
                for num in range(int(start),eval(end)+1):
                    ret.append( '%0*d'%(len(start),num) )
            elif bb == '0':
                for num in range(eval('0%s'%(start,)),eval(end)+1):
                    ret.append( '%0*o'%(len(start),num) )
            elif bb == '0x':
                for num in range(eval('0x%s'%(start,)),eval(end)+1):
                    ret.append( '%0*x'%(len(start),num) )
        else: ret.append( sss )
    TRACE( 28, 'numeric_expand returning %s', ret )
    return ret
    # numeric_expand

def expand( ss ):
    import string
    import re
    TRACE( 29, 'expand(%s)', ss )
    ssIn = ss
    try:
        placeholder_idx = 0
        expands = findall_expands( ss )
        if not expands: return ss.split(',')
        exp_result = []
        for exp in expands:
            ss = ss.replace( exp, '<%d>'%(placeholder_idx,), 1 )
            placeholder_idx = placeholder_idx + 1
        placeholder_idx = 0
        for sss in ss.split(','):
            TRACE( 30, 'expand sss=%s of ss=%s', sss, ss )
            place_holders = re.findall( '<[0-9]+>', sss )
            for idx in range(len(place_holders)):
                p_holder = '<%d>'%(placeholder_idx+idx,)
                expanding = expand( expands[placeholder_idx+idx][1:-1] ) #Recursive call
                expanding = numeric_expand( expanding )
                result = []
                for ssss in sss.split(','):
                    holder_idx = ssss.find(p_holder)
                    if holder_idx != -1:
                        pre = ssss[:holder_idx]
                        post= ssss[holder_idx+len(p_holder):]
                        for expanded in expanding:
                            result.append( pre+expanded+post )
                sss = ','.join(result)
            exp_result = exp_result + sss.split(',')
            placeholder_idx = placeholder_idx + len(place_holders)
    except:  # any
        TRACE( 31, 'except - expand' )
        exc, value, tb = sys.exc_info()
        sys.stderr.write('Error expanding node list "%s": %s: %s\n'%(ssIn,exc,value) )
        sys.stderr.write('Prehaps an invalid decimal/octal/hex digit\n' )
        sys.stderr.write('remember: in the \'{seq1-seq2}\' syntax, seq2\n' )
        sys.stderr.write('can begin with \'0x\' to force hex or \'0\' to\n' )
        sys.stderr.write('force octal\n' )
        if g_opt['tlvlmsk']:
            for ln in traceback.format_exception( exc, value, tb ):
                sys.stderr.write(ln)
        sys.exit(1)

    return exp_result
    # expand, numeric_expand, findall_expands


def build_quoted_str( args ):
    import string                       # join
    quoted_args=[]
    for arg in args:
        if repr(arg)[0] == "'": quoted_args.append( "'%s'"%(arg,) )
        else:                   quoted_args.append( '"%s"'%(arg,) )
    return ' '.join( quoted_args )
    # build_quoted_str



def main():
    import getopt
    art_args=''
    if sys.argv.count('--'):
	art_args = build_quoted_str( sys.argv[sys.argv.index('--' ) + 1:] )
	pass
    long_opt_spec=["help","ddnodes=","nodes="]
    try:
        opts, args = getopt.gnu_getopt(sys.argv[1:], "h", long_opt_spec)
    except getopt.GetoptError, err:
        # print help information and exit:
        print str(err) # will print something like "option -a not recognized"
        print(USAGE)
        sys.exit(2)
    ddnodes=0
    nodes=[]
    for o, a in opts:
        if o in ("-h", "--help"):
            usage()
            sys.exit()
        elif o in ("--ddnodes",):
            ddnodes = int(a,0)
        elif o in ("--nodes",):
            nodes = expand(a)
        else:
            assert False, "unhandled option"


    if len(args) < 4: print(USAGE); sys.exit()    

    num_det    = int( args[0],0 )
    num_sink   = int( args[1],0 )

    builder=os.popen("which builder 2>/dev/null").readline()
    if not builder:
	print("Error: builder executable not found.")
	sys.exit(3)
	pass

    print("opts=%s args=%s nodes=%s"%(opts,args,nodes))
    rc = 0
    if nodes:
	if ddnodes >= len(nodes):
	    print("invalid configuration. ddnodes must be < nodes")
	    sys.exit(4)
	    pass
	# create a nodes file... The number of lines should be == num_det+num_src+num_sink

	nodes_file="/tmp/nodes%s.txt"%(os.getpid(),)
	fo=open(nodes_file,'w')
	
	# 1st set of ranks is for "detector" nodes
	node_idx=0
	for xx in range(num_det):
	    fo.write( "%s\n"%(nodes[node_idx],) ); node_idx += 1
	    if node_idx == len(nodes) or node_idx == ddnodes: node_idx=0
	    pass
	if ddnodes: nodes=nodes[ddnodes:]

	node_idx=0   # dect and src get paired up
	
	# last set of ranks is for "sink" nodes
	for xx in range(num_sink):
	    fo.write( "%s\n"%(nodes[node_idx],) ); node_idx += 1
	    if node_idx == len(nodes): node_idx=0
	    pass

	fo.close()

	os.system("cat %s"%(nodes_file,))
	#os.remove( nodes_file )

	cmd ="mpirun_rsh -rsh -hostfile %s "%(nodes_file,)
	cmd+="-n %d "%(num_det+num_sink,)
        cmd+=" FHICL_FILE_PATH=\"$FHICL_FILE_PATH\" "
	cmd+=builder[:-1]
	if art_args: cmd+=" %s"%(art_args,)
	print( "executing cmd: %s"%(cmd,) )
	os.system(cmd)
    else:
	cmd ="mpirun -n %d "%(num_det+num_sink,)
	cmd+=builder[:-1]
	if art_args: cmd+=" %s"%(art_args,)
	print( "executing cmd: %s"%(cmd,) )
	rc = (os.system( cmd ) >> 8)
        print "return status of %s is: %d\n" %(cmd, rc)

    print "return status is (really!) : %d\n" %(rc,)
    sys.exit(rc)


if __name__ == "__main__":
    main()
