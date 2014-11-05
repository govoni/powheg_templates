#!/usr/bin/python

import commands
import fileinput
import sys
import os


rootfolder = os.getcwd()


def replaceAll(file,searchExp,replaceExp):
    for line in fileinput.input(file, inplace=1):
        if searchExp in line:
            line = line.replace(searchExp,replaceExp)
        sys.stdout.write(line)


# ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----


def prepareJob (tag, i, folderName) :
    filename = 'run_' + tag + '.job'
    f = open (filename, 'w')
    f.write ('cd ' + rootfolder + '\n')
    f.write ('source setup.sh' + '\n')
    f.write ('cd ' + folderName + '\n')
#    f.write ('cd POWHEG-BOX/Govoni/' + folderName + '\n')
    f.write ('echo ' + str (i) + ' | ../pwhg_main > log_' + tag + '.log 2>&1' + '\n')
    f.close ()
    return filename


# ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----


def prepareJobForEvents (tag, i, folderName, EOSfolder) :
    commands.getstatusoutput ('rm ' + rootfolder + '/' + folderName + '/log_' + tag + '.log')
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
    f.write ('cmsStage ' + lhefilename + ' /store/user/govoni/LHE/powheg/7TeV/' + EOSfolder + '\n')
    f.write ('rm ' + lhefilename + '\n')
    f.close ()
    return filename


# ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----


def runWithXgrid (parstage, xgrid, folderName, njobs, powInputName, jobtag) :
    print 'submitting jobs'
    commands.getstatusoutput ('rm ' + folderName + 'powheg.input')
    sedcommand = 'cat ./' + powInputName + ' | sed "s/parallelstage.*/parallelstage ' + parstage + '/ ; s/xgriditeration.*/xgriditeration ' + xgrid + '/">powheg.input'
    commands.getstatusoutput (sedcommand)
    commands.getstatusoutput ('cp powheg.input ' + folderName)
    commands.getstatusoutput ('mv powheg.input ' + folderName + '/powheg.input.' + parstage + '_' + str (xgrid))
    for i in range (1, njobs + 1) :
        tag = parstage + '_' + str (i) + '_' + str (xgrid)
        jobname = prepareJob (tag, i, folderName)
        jobID = jobtag + '_' + str (i)
        print ('bsub -J ' + jobID + ' -u pippopluto -q 2nw < ' + jobname)
        #print ('    jobs not submitted')
        commands.getstatusoutput ('bsub -J ' + jobID + ' -u pippopluto -q 2nw < ' + jobname)
    commands.getstatusoutput ('mv *.job ' + folderName)


# ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----


def run (parstage, folderName, EOSfolder, njobs, powInputName, jobtag) :
    print 'run : submitting jobs'
    commands.getstatusoutput ('rm ' + folderName + 'powheg.input')
    sedcommand = 'cat ./' + powInputName + ' | sed "s/parallelstage.*/parallelstage ' + parstage + '/">powheg.input'
    commands.getstatusoutput (sedcommand)
    commands.getstatusoutput ('cp powheg.input ' + folderName)
    commands.getstatusoutput ('mv powheg.input ' + folderName + '/powheg.input.' + parstage)
    for i in range (1, njobs + 1) :
        tag = jobtag + '_' + str (i)
        if parstage == '4' : jobname = prepareJobForEvents (tag, i, folderName, EOSfolder)
        else               : jobname = prepareJob (tag, i, folderName)
        jobID = jobtag + '_' + str (i)
        print ('bsub -J ' + jobID + ' -u pippopluto -q 2nw < ' + jobname)
        #print ('    jobs not submitted')
        commands.getstatusoutput('bsub -J ' + jobID + ' -u pippopluto -q 2nw < ' + jobname)
    commands.getstatusoutput ('mv *.job ' + folderName)


# ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----


if __name__ == "__main__":

    folderName = 'test_prod'
    parstage      = sys.argv[1]
    xgrid         = sys.argv[2]
    folderName    = sys.argv[3] # grids folder, w/o the higgs mass
    totEvents     = sys.argv[4]
    higgsMass     = sys.argv[5]
    higgsWidth    = sys.argv[6]
    inputTemplate = sys.argv[7] # powheg.input.ct10nlo_MW
    
    genRange = str (round (min ((float (higgsMass) - 50.) / float(higgsWidth), 15), 2))
    
    folderName = folderName + '_' + higgsMass

    if (len(sys.argv) == 9) :
        EOSfolder = sys.argv[8] + '_' + higgsMass
    else :
        EOSfolder = folderName

    print
    print 'RUNNING PARAMS: parstage = ' + parstage + ' , xgrid = ' + xgrid  + ' , folderName = ' + folderName 
    print '                totEvents = ' + totEvents + ' , mH = ' + higgsMass  + ' , width = ' + higgsWidth 
    print '                genRange : ' + genRange 
    print '                powheg input template : ' + inputTemplate 
    print '                working folder : ' + folderName
    print '                EOS folder : ' + EOSfolder
    print '                base folder : ' + rootfolder
    print

#    sys.exit(1)

    res = commands.getstatusoutput ('ls ' + folderName)
    if parstage == '1' and xgrid == '1' and res[0] == 0 :
        print 'folder ' + folderName + ' existing, exiting'
        sys.exit (res[0])
    if parstage == '1' and xgrid == '1' :
        commands.getstatusoutput ('mkdir ' + folderName)
        commands.getstatusoutput ('cp pwgseeds.dat ' + folderName)
    if parstage == '4' :    
        commandOutput = commands.getstatusoutput ('/afs/cern.ch/project/eos/installation/cms/bin/eos.select mkdir /eos/cms/store/user/govoni/LHE/powheg/7TeV/' + EOSfolder)
        print commandOutput

    njobs = int (totEvents) / 2000
    #PG 2000 should appear in powheg.input as numevts 
    #njobs = int (totEvents) / 40
    #print "\n\n WARNING RUNNING WITH 40 EVENTS PER JOB!!!!!!\n\n"

    # pass the powheg.input to the scripts
    powInputName = 'powheg.input-template_7TeV_' + higgsMass
    commands.getstatusoutput ('cp ' + inputTemplate + ' ' + powInputName)
    replaceAll (powInputName, 'HIGGSMASS', higgsMass)
    replaceAll (powInputName, 'HIGGSWIDTH', higgsWidth)
#    genrange = min (int (higgsMass / higgsWidth), 15)
    replaceAll (powInputName, 'GENRANGE', genRange)

    jobtag = higgsMass + '_' + parstage + '_' + xgrid
    if parstage == '1' : runWithXgrid (parstage, xgrid, folderName, njobs, powInputName, jobtag)
    else               : run (parstage, folderName, EOSfolder, njobs, powInputName, jobtag)

