#!/usr/bin/python

import commands
import fileinput
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
    f.write ('cd ' + rootfolder + '\n')
    f.write ('source setup.sh' + '\n')
#    f.write ('cd -\n')
    f.write ('cd ' + folderName + '\n')
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
    f.write ('cd ' + rootfolder + '\n')
    f.write ('source setup.sh' + '\n')
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
    f.write ('cmsStage ' + lhefilename + ' /store/user/govoni/LHE/powheg/14TeV/' + EOSfolder + '\n')
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

    folderName = 'test_prod'
    parstage      = sys.argv[1]
    xgrid         = sys.argv[2]
    folderName    = sys.argv[3] # grids folder
    totEvents     = sys.argv[4]
#    higgsMass     = sys.argv[X]
#    higgsWidth    = sys.argv[X]
    inputTemplate = sys.argv[5] # FIXME build the template... it simply should be the cfg file
    
#    genRange = str (round (min ((float (higgsMass) - 50.) / float(higgsWidth), 15), 2))
#    folderName = folderName + '_' + higgsMass

    EOSfolder = folderName

    print
    print 'RUNNING PARAMS: parstage = ' + parstage + ' , xgrid = ' + xgrid  + ' , folderName = ' + folderName 
    print '                totEvents = ' + totEvents 
    print '                powheg input cfg file : ' + inputTemplate 
    print '                working folder : ' + folderName
    print '                EOS folder : ' + EOSfolder
    print '                base folder : ' + rootfolder
    print
 
    if (TESTING == 1) :     
        print '  --- TESTNG, NO submissions will happen ---  '
        print

    res = runCommand ('ls ' + folderName)
    if parstage == '1' and xgrid == '1' and res == 0 :
        print 'folder ' + folderName + ' existing, exiting'
        sys.exit (1)
    if parstage == '1' and xgrid == '1' :
        runCommand ('mkdir ' + folderName)
        runCommand ('cp pwgseeds.dat ' + folderName)
#        #FIXME this is a crude hardcoded trick to overcome some problems in LHAPDF usage
#        runCommand ('ln -s /afs/cern.ch/user/g/govoni/work/HiggsPlusJets/lhapdf/share/lhapdf/PDFsets/CT10.LHgrid ./'  + folderName)
    if parstage == '4' :    
        runCommand (eoscmd + ' mkdir /eos/cms/store/user/govoni/LHE/powheg/14TeV/' + EOSfolder, 1, 1)

    njobs = int (totEvents) / 2000
    #PG 2000 should appear in powheg.input as numevts 
    #PG FIXME put a cross-check or read the nuimber from the powheg input file
    #njobs = int (totEvents) / 40
    #print "\n\n WARNING RUNNING WITH 40 EVENTS PER JOB!!!!!!\n\n"

    powInputName = inputTemplate  # FIXME nothing needs to be modified

    jobtag = parstage + '_' + xgrid
    if parstage == '1' : runWithXgrid (parstage, xgrid, folderName, njobs, powInputName, jobtag)
    else               : run (parstage, folderName, EOSfolder, njobs, powInputName, jobtag)
