#!/usr/bin/python

import commands
import fileinput
import argparse
import sys
import os

TESTING = 0
QUEUE = '2nw'

rootfolder = os.getcwd ()


def runCommand (command, printIt = 0, doIt = 1) :
    if printIt : print ('> ' + command)
    if doIt : 
        commandOutput = commands.getstatusoutput (command)
        if printIt : print commandOutput[1]
        return commandOutput[0]
    else :    print ('    jobs not submitted')
    return 1
    

# ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----


def replaceAll (file,searchExp,replaceExp) :
    for line in fileinput.input (file, inplace = 1) :
        if searchExp in line:
            line = line.replace (searchExp, replaceExp)
        sys.stdout.write (line)


# ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----


def prepareJob (tag, i, folderName) :
    filename = 'run_' + tag + '.job'
    f = open (filename, 'w')

    f.write ('cd /afs/cern.ch/user/g/govoni/CMSSW_7_2_0/src/                                              \n')
    f.write ('eval `scram run -sh`                                                                        \n')
    f.write ('export FASTJET_BASE=`scram tool info fastjet | grep FASTJET_BASE | sed -e s%FASTJET_BASE=%%`\n')
    f.write ('export PATH=$FASTJET_BASE/bin/:$PATH                                                        \n')
    f.write ('export LHAPDF_BASE=`scram tool info lhapdf | grep LHAPDF_BASE | sed -e s%LHAPDF_BASE=%%`    \n')
    f.write ('export PATH=$LHAPDF_BASE/bin/:$PATH                                                         \n')
    f.write ('export LHAPATH=`scram tool info lhapdf | grep LHAPATH | sed -e s%LHAPATH=%%`                \n')

    f.write ('cd ' + rootfolder + '/' + folderName + '\n')
    f.write ('echo ' + str (i) + ' | ../pwhg_main > log_' + tag + '.log 2>&1' + '\n')
#    f.write ('cp * ' + rootfolder + '/' + folderName + '\n')
    f.close ()
    return filename


# ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----


def prepareJobForEvents (tag, i, folderName, EOSfolder) :
    runCommand ('rm ' + rootfolder + '/' + folderName + '/log_' + tag + '.log')
    filename = 'run_' + tag + '.job'
    f = open (filename, 'w')
    f.write ('cp ' + rootfolder + '/' + folderName + '/powheg.input ./' + '\n')
    f.write ('cp ' + rootfolder + '/' + folderName + '/*.dat  ./' + '\n')

    f.write ('cd /afs/cern.ch/user/g/govoni/CMSSW_7_2_0/src/                                              \n')
    f.write ('eval `scram run -sh`                                                                        \n')
    f.write ('export FASTJET_BASE=`scram tool info fastjet | grep FASTJET_BASE | sed -e s%FASTJET_BASE=%%`\n')
    f.write ('export PATH=$FASTJET_BASE/bin/:$PATH                                                        \n')
    f.write ('export LHAPDF_BASE=`scram tool info lhapdf | grep LHAPDF_BASE | sed -e s%LHAPDF_BASE=%%`    \n')
    f.write ('export PATH=$LHAPDF_BASE/bin/:$PATH                                                         \n')
    f.write ('export LHAPATH=`scram tool info lhapdf | grep LHAPATH | sed -e s%LHAPATH=%%`                \n')

#    f.write ('cd ' + rootfolder + '\n')
#    f.write ('source setup.sh' + '\n')

    f.write ('cd -' + '\n')
    f.write ('pwd' + '\n')
    f.write ('ls' + '\n')
    f.write ('echo ' + str (i) + ' | ' + rootfolder + '/pwhg_main > log_' + tag + '.log 2>&1' + '\n')
    f.write ('cp log_' + tag + '.log ' + rootfolder + '/' + folderName + '\n')
    lhefilename = 'pwgevents-' 
    if i < 10     : lhefilename = lhefilename + '000' + str (i) + '.lhe'
    elif i < 100  : lhefilename = lhefilename + '00' + str (i) + '.lhe'
    elif i < 1000 : lhefilename = lhefilename + '0' + str (i) + '.lhe'
    else          : lhefilename = lhefilename + str (i) + '.lhe'
    f.write ('cmsStage ' + lhefilename + ' /store/user/govoni/LHE/powheg/' + EOSfolder + '/\n')
    f.write ('rm ' + lhefilename + '\n')
    f.close ()
    return filename


# ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----


def runWithXgrid (parstage, xgrid, folderName, njobs, powInputName, jobtag) :
    print 'submitting jobs'
    runCommand ('rm ' + folderName + 'powheg.input')
    sedcommand = 'cat ./' + powInputName + ' | sed "s/parallelstage.*/parallelstage ' + parstage + '/ ; s/xgriditeration.*/xgriditeration ' + xgrid + '/">powheg.input'
    runCommand (sedcommand)
    if (parstage == '1') :
        runCommand ("echo \'fakevirt 1\' >> powheg.input")
    runCommand ('cp powheg.input ' + folderName)
    runCommand ('mv powheg.input ' + folderName + '/powheg.input.' + parstage + '_' + str (xgrid))
    for i in range (1, njobs + 1) :
        tag = jobtag + '_' + str (i)
        jobname = prepareJob (tag, i, folderName)
        jobID = jobtag + '_' + str (i)
        runCommand ('bsub -J ' + jobID + ' -u pippopluto -q ' + QUEUE + ' < ' + jobname, 1, TESTING == 0)
    runCommand ('mv *.job ' + folderName)


# ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----


def run (parstage, folderName, EOSfolder, njobs, powInputName, jobtag) :
    print 'run : submitting jobs'
    runCommand ('rm ' + folderName + 'powheg.input')
    sedcommand = 'cat ./' + powInputName + ' | sed "s/parallelstage.*/parallelstage ' + parstage + '/ ; s/xgriditeration.*/xgriditeration 1/">powheg.input'
    runCommand (sedcommand)
    runCommand ('cp powheg.input ' + folderName)
    runCommand ('mv powheg.input ' + folderName + '/powheg.input.' + parstage)
    for i in range (1, njobs + 1) :
        tag = jobtag + '_' + str (i)
        if parstage == '4' : jobname = prepareJobForEvents (tag, i, folderName, EOSfolder)
        else               : jobname = prepareJob (tag, i, folderName)
        jobID = jobtag + '_' + str (i)
        runCommand ('bsub -J ' + jobID + ' -u pippopluto -q ' + QUEUE + ' < ' + jobname, 1, TESTING == 0)
    runCommand ('mv *.job ' + folderName)


# ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----


if __name__ == "__main__":

    eoscmd = '/afs/cern.ch/project/eos/installation/cms/bin/eos.select' ;

#    folderName = 'test_prod'
#    parser.parstage      = sys.argv[1]
#    xgrid         = sys.argv[2]
#    folderName    = sys.argv[3] # grids folder
#    totEvents     = sys.argv[4]
#    inputTemplate = sys.argv[5] # FIXME build the template... it simply should be the cfg file
#    eosFolderName = sys.argv[6]
    
    parser = argparse.ArgumentParser (description = 'run phantom productions on lxplus')
    parser.add_argument('-p', '--parstage'      , default= '1',            help='stage of the production process [1]')
    parser.add_argument('-x', '--xgrid'         , default= '1',            help='loop number for the girds production [1]')
    parser.add_argument('-f', '--folderName'    , default='testProd',      help='local folder and last eos folder name[testProd]')
    parser.add_argument('-e', '--eosFolder'     , default='NONE' ,         help='folder before the last one, on EOS')
    parser.add_argument('-t', '--totEvents'     , default= '10000',        help='total number of events to be generated [10000]')
    parser.add_argument('-n', '--numEvents'     , default= '2000',         help='number of events for a single job [2000]')
    parser.add_argument('-i', '--inputTemplate' , default= 'powheg.input', help='input cfg file (fixed) [=powheg.input]')
    parser.add_argument('-q', '--lsfQueue'      , default= '2nw',          help='LSF queue [2nw]')

    args = parser.parse_args ()
    
    QUEUE = args.lsfQueue
    EOSfolder = args.folderName

    print
    print 'RUNNING PARAMS: args.parstage = ' + args.parstage + ' , args.xgrid = ' + args.xgrid  + ' , args.folderName = ' + args.folderName 
    print '                args.totEvents = ' + args.totEvents 
    print '                powheg input cfg file : ' + args.inputTemplate 
    print '                working folder : ' + args.folderName
    print '                EOS folder : ' + args.eosFolder + '/' + EOSfolder
    print '                base folder : ' + rootfolder
    print
 
    if (TESTING == 1) :     
        print '  --- TESTNG, NO submissions will happen ---  '
        print

    res = runCommand ('ls ' + args.folderName)
    if args.parstage == '1' and args.xgrid == '1' and res == 0 :
        print 'folder ' + args.folderName + ' existing, exiting'
        sys.exit (1)
    if args.parstage == '1' and args.xgrid == '1' :
        runCommand ('mkdir ' + args.folderName)
        runCommand ('cp pwgseeds.dat ' + args.folderName)
#        #FIXME this is a crude hardcoded trick to overcome some problems in LHAPDF usage
#        runCommand ('ln -s /afs/cern.ch/user/g/govoni/work/HiggsPlusJets/lhapdf/share/lhapdf/PDFsets/CT10.LHgrid ./'  + args.folderName)
    if args.parstage == '4' :    
        runCommand (eoscmd + ' mkdir /eos/cms/store/user/govoni/LHE/powheg/' + args.eosFolder, 1, 1)
        runCommand (eoscmd + ' mkdir /eos/cms/store/user/govoni/LHE/powheg/' + args.eosFolder + '/' + EOSfolder, 1, 1)

    njobs = int (args.totEvents) / int (args.numEvents)

    sedcommand = 'cat ./' + args.inputTemplate + ' | sed "s/numevts.*/numevts ' + args.numEvents + '/">' + args.inputTemplate + '_tempo'
    runCommand (sedcommand)
    powInputName = args.inputTemplate + '_tempo'

    jobtag = args.parstage + '_' + args.xgrid
    if args.parstage == '1' : runWithXgrid (args.parstage, args.xgrid, args.folderName, njobs, powInputName, jobtag)
    else                    : run (args.parstage, args.folderName, args.eosFolder + '/' + EOSfolder, njobs, powInputName, jobtag)
