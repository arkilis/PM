#!/usr/bin/env python

import sys, os
import shutil
import MySQLdb as mdb
import time

G="<font color='green'>"
R="<font color='red'>"  
E="</font><br />"


# get the current date, return a string
def getDate():
    return time.strftime("%Y%m%d", time.localtime())

# get subfolders under the maindir + current Date
# return a list containing subfolders' full path
def getSubDirs(dir):
    ay_res= []
    
    if(os.path.exists(dir)==True):
        for sub in os.listdir(dir):
            ay_res.append(dir+"/"+sub)

    return ay_res

# get MD5 of a file
def getMD5(filename):
    p= os.popen("md5sum "+filename)
    sztmp= p.read()
    # use two spaces to split
    ay_tmp= sztmp.split("  ")
    return ay_tmp[0]

# Function: check md5 by running md5sum
# md5file: md5file path
# suddir: the parent directory of md5file
# return True or False
def checkMD5(md5file, subdir):
    count=0
    fmd5=open(md5file)
    cMD5= 0
    
    for f in fmd5.readlines():
        if(len(f)!=0):  # avoid empty line
            cMD5+=1
            ay_f= f.split("  ")
            
            szMD5= ay_f[0].replace("\n","")
            szFilename= ay_f[1].replace("\n","")

            if(szMD5==getMD5(subdir+"/"+szFilename)):
            #if(szMD5==getMD5(szFilename)):
                count+=1    
        
    if(count==cMD5):
        return True
    else:
        return False 

# check done
# return a list of undone subfolders
def checkDone(ay_subdirs):
    ay_res=[]
    for elm in ay_subdirs:
        if(os.path.exists(elm+"/"+"done.txt")!=True):
            ay_res.append(elm)
        else:
			print R+getDateTime()+": Have ran TurboMetaDB before or the "+elm+"/done.txt exist, if you are sure to run TurboMetaDB, please delete done.txt ..."+E
    return ay_res
           
# Functions:
# 1, check user's csv existing, 2, contain the data file or not 3, support multiple line of user.csv
# return true or false
def checkUsercsv(csvfile):
    if(os.path.exists(csvfile)==True):
        fcsv= open(csvfile)
        ay_file= []

        for line in fcsv.readlines():
            ay_line= line.split(",")
            sztmp1=ay_line[9].replace("\n","")
            sztmp1=sztmp1.replace(" ", "")
            sztmp2=ay_line[10].replace("\n","")
            sztmp2=sztmp2.replace(" ", "")
            ay_file.append(sztmp1)
            ay_file.append(sztmp2)

        # remove dupliated elements
        ay_file=list(set(ay_file))

        count=0

        for elm_path in ay_file:
            if(os.path.exists(elm_path)==True):
                count+=1
                
        fcsv.close()
        
        if(count==len(ay_file)):
            return True
        else:
            return False
    else:
        return False

# get total number of user's csv file
# return an integer
def getCSVNum(usercsv):
    if(os.path.exists(usercsv)==False):
        print "Can not find user.csv file."
        return 0
    else:
        f=open(usercsv)    
        count=0
        lines=f.readlines()
        for elm in lines:
            count+=1

        return count

# Split user'csv file. Each line can be treated as a new user.csv
# create many user.csv file followint the naming: user1.csv user2.csv etc
# return a list of new user csv files
def splitCSV(usercsv):
    
    usercsvs=[]
    count=1
    
    if(os.path.exists(usercsv)!=False):
        f=open(usercsv)
        lines= f.readlines()
        dirpath= os.path.dirname(usercsv)
        
        for elm in lines:
            szTmp= dirpath+"/user"+str(count)+".csv"
            ftmp= open(szTmp,"w")
            ftmp.write(elm)
            ftmp.close()
            count+=1
            usercsvs.append(szTmp)
        
        return usercsvs
    else:
        return usercsvs
    
# get the lane number from user's csv
# return an int    
def getLaneNum(csv):
    ay_res= []
    fcsv= open(csv)
    
    for elm in fcsv.readlines():
        ay_elm= elm.split(",")
        ay_res.append(ay_elm[0])

    fcsv.close()
    return ay_res[0]
        
# get soapfile name (full path) from user's csv
# return an array
def getRawFilePath(csv):

    ay_res= []
    fcsv= open(csv)
    
    for elm in fcsv.readlines():
        ay_elm= elm.split(",")
        ay_res.append(ay_elm[9])
        ay_res.append(ay_elm[10])
    fcsv.close()
    return ay_res

# run TurboMetaDB
def runTMDB(pipeline, usercsv, szLibName, szFormat, iReadlimits):
    
    print G+getDateTime()+": Running TurboMetaDB ..."+E
    CMD= pipeline+" -csv "+usercsv
    CMD+=" -library_name "+ szLibName
    CMD+=" -format "+szFormat
    CMD+=" -readslimit "+str(iReadlimits)+" -debug -sanger -uncompressed -noautofill >/dev/null"
    os.system(CMD)
    print G+getDateTime()+": Running TurboMetaDB Done!"+E


# make done flag
def makeDone(subdir):
    # make a flag
    f=open(subdir+"/done.txt","w")
    f.close()
    
# NOTICE: suppose each time just run the first line of the user.csv
# tubodir: should be the base direcotry of turbometadb++/MetaDB_processing/plots
# return an array of quality image names
def makeQuPngNames(tubodir, usercsv):
    
    ay_res=[]
    f= open(usercsv)

    for line in f.readlines():
        ay_line= line.split(",")
        laneNum= ay_line[0]
        culName= ay_line[3]
        soapFullName1= ay_line[9].replace("\n","")
        ay_soap1= os.path.basename(soapFullName1).split(".")
        soap1=ay_soap1[0]
        soapFullName2= ay_line[10].replace("\n","")
        ay_soap2= os.path.basename(soapFullName2).split(".")
        soap2=ay_soap2[0]
        ay_res.append(tubodir+ "/"+getDate()[2:]+"/quality_plots/"+getDate()[2:]+"_lane_"+laneNum+"_"+soap1+".png")
        ay_res.append(tubodir+ "/"+getDate()[2:]+"/quality_plots/"+getDate()[2:]+"_lane_"+laneNum+"_"+soap2+".png")

    return ay_res
         

# check the existance
def checkExtQuPng(ay_qupngs):

    count=0
    if(len(ay_qupngs)==0):
        print "Invalid Quality Names.."
        return False
    else:
        for elm in ay_qupngs:
            os.path.exists(elm)
            count+=1
            
    if(count==len(ay_qupngs)):
        return True
    else:
        return False
        
# run R
# csvname: csv file full path
# resname: res file (png) full path
# subdir: e.g. mainDir/20121112/201211120900/
def r(csvname, resname, subdir):
   
    print G+getDateTime()+": Running R script..."+E
     
    f=open(subdir+"/tmp.r", "w")
    CMD= 'data <- read.csv(file = "'+csvname+'",sep=",",header=FALSE);'
    f.write(CMD)
    f.write("\n")

    CMD='png(filename="'+resname+'",width=1200,height=500);'
    f.write(CMD)
    f.write("\n")

    CMD='hist(data$V1,breaks = 100000, main = "Histogram", xlab = "Insert Size", ylab = "Occurances (Frequency) of Insert Size", xlim = c(0, 600));'
    CMD+='dev.off();'
    f.write(CMD)
    f.write("\n")
    f.close()
    os.system("R CMD BATCH "+subdir+"/tmp.r")

# zip small files
# ay_Qunames: an array of all of the quality names
# szIsname: the name of the insert size png
# return the tar.gz file full path

def collect(subdir, ay_Qunames, szIsname):
    
    # create a new folder inside the sub dir
    cmd= subdir+"/tmp"
    os.system("mkdir -p "+cmd)

    # copy quality pngs to folder
    for qu in ay_Qunames:
        fname= os.path.basename(qu)
        shutil.copyfile(qu, cmd+"/"+fname)       

    # copy insert size pngs to folder
    isName=os.path.basename(szIsname)
    shutil.copyfile(szIsname, cmd+"/"+isName)

    # compress folder
    os.system("tar -cvzf "+cmd +"/smallfiles.tar.gz "+ cmd+"/")
    
    return cmd+"/smallfiles.tar.gz"


########################################################################
 
def getReadLen(file_fastq):
    if(os.path.exists(file_fastq)==True):
        print G+getDateTime()+": Caculating read Length ..."+E
        cmd="head -n 2 "+file_fastq+" |tail -n 1 |wc -c >/tmp/readlengthtmp.txt"
        os.system(cmd)
        fhandle= open("/tmp/readlengthtmp.txt")
        res= fhandle.readline()
        res=res.replace("\n", "")
        res=res.replace(" ", "")
        res=str(int(res)-1)
        fhandle.close()
        os.remove("/tmp/readlengthtmp.txt")
        print G+getDateTime()+": The Read Length is "+res+"."+E
        return res

# make up fastq name in user.csv
# 
def makeFastqName(filename):
    if(filename!="" and filename.find(".")!=-1):
        return filename[:filename.find(".")]+".fastq"
    else:
        return ""
        
# decompress and keep the gz files
def decompress(gzfile):
    gzfile=gzfile.replace(" ","")
    gzfile=gzfile.replace("\n","")

    if(os.path.exists(gzfile)==True):
        print G+getDateTime()+": Decompressing "+gzfile+E
        fastqfile=""
        if(gzfile.find(".fastq")!=-1):
            fastqfile=gzfile[:gzfile.find(".fastq")+6]
        else:
            fastqfile=gzfile.replace(".gz", ".fastq")

        cmd="gzip -c -d "+gzfile+">"+fastqfile
        os.system(cmd)
        print G+getDateTime()+": Decompress done!"+E
    else:
        print R+getDateTime()+": Can't find gz files: "+gzfile+E

# multiple purpose decomress
# return file extension
def decompress2(cfile):
    cfile= cfile.replace(" ","")
    cfile= cfile.replace("\n","")
    ext=""

    if(os.path.exists(cfile)==True):
        # get extended file name
        ext= cfile.split(".")[-1]

        dirPath= os.path.dirname(cfile)
        print G+getDateTime()+": File type "+ext+" Found!"+E
        print G+getDateTime()+": Decompressing..."+E
        fastqfile=makeFastqName(cfile)
        cmd="/home/miseq01/bin/myunzip -o "+dirPath+" "+cfile
        print cmd
        os.system(cmd)
        print G+getDateTime()+": Decompress done!"+E
        #return ext
    else:
        print R+getDateTime()+": Can't find compressed files: "+cfile+E
        #return ext

# complete user.csv file
def cmplCSV(csvPath):
    if(os.path.exists(csvPath)==True):
        print G+getDateTime()+": Fullfill CSV file... "+E
        fcsv= open(csvPath)
        line= fcsv.readline()
        ay_line= line.split(",")
        fcsv.close()

        # decompress
        decompress2(ay_line[9])
        decompress2(ay_line[10])

        # make fastq file name
        fastq1= makeFastqName(ay_line[9])
        fastq2= makeFastqName(ay_line[10])
        ay_line[9]=fastq1
        ay_line[10]=fastq2

        """
        if(ay_line[9].find(".fastq")!=-1):
            fastq1= ay_line[9][:ay_line[9].find(".fastq")+6]
            ay_line[9]=fastq1
        else:
            fastq1= ay_line[9].replace(ext, ".fastq")
            ay_line[9]=fastq1

        if(ay_line[10].find(".fastq")!=-1):
            fastq2= ay_line[10][:ay_line[10].find(".fastq")+6]
            ay_line[10]=fastq2
        else:
            fastq2= ay_line[10].replace(ext, ".fastq")
            ay_line[10]=fastq2
        """
        
        readLen=""
        if(os.path.exists(ay_line[9])==True):
            # get read length
            readLen= getReadLen(fastq1)
            ay_line[7]= readLen

        # write comments to a temp file
        cmts= ay_line[11]
        cmtsPath= os.path.dirname(csvPath)+"/cmts.txt" 
        fCmts= open(cmtsPath, "w")
        fCmts.write(cmts)
        fCmts.close()
  

        # fill missing columns and remove the comments field and write back to user.csv
        fcsv= open(csvPath,"w")
        ay_line.pop()
        szCSV= ",".join(ay_line)
        fcsv.write(szCSV)
        fcsv.close()
        print G+getDateTime()+": Fullfill CSV file done! "+E
    else:
        print R+getDateTime()+": user.csv not exists!"+E 
    
# automatically select the avaiable barrine node from node 1 to node 3
# need the test.sh which is under the /home/uqyliu19/ on barrine
# return 1,2,3

def autoSel():

    b=[1,2,3]
    for b_elm in b:
        f=os.popen("ssh uqyliu19@barrine"+str(b_elm)+".hpcu.uq.edu.au 'sh test.sh 1'")
        res=f.read()
        if(res.replace("\n","")!=""):
            time.sleep(60)
            print G+getDateTime()+": Select barrine"+str(b_elm)+".hpcu.uq.edu.au as the uploading\n"+E
            return b_elm


# make
def makeRemoteFolder(bNum, sname, cname, subdir, destdir="/HPC/groups/NCISF/MAS/W26/jarrah/rawData/"):
    
    # build dir
    ay_subdir= subdir.split("/")
    mydir=destdir+sname+"/"+cname+"/"+ay_subdir[-2]+"/"+ay_subdir[-1]
    mydir=mydir.replace(" ","_")
    
    print "ssh uqyliu19@barrine"+str(bNum)+".hpcu.uq.edu.au mkdir -p "+mydir
    os.system("ssh uqyliu19@barrine"+str(bNum)+".hpcu.uq.edu.au mkdir -p "+mydir)
    print "pause 10s to wait for feedback...."
    time.sleep(10)
     

# upload file to barrine
# bNum:     barrine number {1,2,3}
# sname:    species name
# cname:    cultivar name
# return the backup path on barrine

def upload(bNum, orgiFile, sname, cname, subdir, destdir="/HPC/groups/NCISF/MAS/W26/jarrah/rawData/"):

    # build upload file full path
    ay_subdir= subdir.split("/")
    mydir=destdir+sname+"/"+cname+"/"+ay_subdir[-2]+"/"+ay_subdir[-1]
    #print "debug: ", mydir
    mydir=mydir.replace(" ","_")
    
    print G+"Uploading file "+orgiFile+ " to "+mydir+E
    CMD="scp "+orgiFile+" uqyliu19@barrine"+str(bNum)+".hpcu.uq.edu.au:'"+mydir+"'"
    #CMD="gzip -c "+orgiFile+"|ssh uqyliu19@barrine"+str(bNum)+".hpcu.uq.edu.au 'gunzip -c >"+mydir+"'"
    os.system(CMD)
    time.sleep(20)
    
    # dmput
    print G+"Put raw data file on tape..."+E
    CMD_dmput="ssh uqyliu19@barrine"+str(bNum)+".hpcu.uq.edu.au 'dmput -r "+mydir+"/"+os.path.basename(orgiFile)+"'"
    os.system(CMD_dmput)
    time.sleep(10)

    # finish
    print G+"Finish uploading raw data file "+orgiFile+E
    return mydir+"/"+os.path.basename(orgiFile)

# localfile: file on localhost
# remotefile: file on barrine
def checkBarrineMD5(localfile, remotefile, bNum ):            
    cmd_local=  "md5sum "+localfile
    cmd_remote= "ssh uqyliu19@barrine"+str(bNum)+".hpcu.uq.edu.au 'md5sum "+remotefile+"'"
    res_local=  os.popen(cmd_local)
    sz_local= res_local.read()
    res_remote= os.popen(cmd_remote)
    sz_remote= res_remote.read()
    
    # get md5 part
    ay_local= sz_local.split("  ")
    ay_remote= sz_remote.split("  ")
    
    if(ay_local[0]==ay_local[0]):
        return True
    else:
        return False
    
# get species name
def getSpeciesName(csv):
    
    f=open(csv) 
    szline= f.readline()
    ay_line= szline.split(",")
    return ay_line[6]

# get cultivar name
# assume each time there is only one cultivar
def getCulName(csv):
    
    f=open(csv)
    
    szline= f.readline()
    ay_line= szline.split(",")
    return ay_line[3]

    
# connect mysql need MysqlDB module 
# return the connection
def conn(url="localhost", username="root", passwd="toor424", dbname="TMDBRes"):
    con= None
    try:
        con = mdb.connect(url, username, passwd, dbname)
        return con
    except mdb.Error, e:
        print "Error %d: %s" % (e.args[0], e.args[1])
        return None


# get bio key by library id
def getBiokey(conn, lid):

    cmd="select c.biosource_library_key from cultivar as c, library as l"
    cmd+=" where c.cid=l.cid and l.lid="+str(lid)
	
    with conn:
        cur = conn.cursor()
        cur.execute(cmd)
        res= cur.fetchone()
        return res[0]
    
        
# get the Estimated length
# return a dictionary
def getEstLength(configFile="/home/miseq01/bin/TurboMetaDB/MetaDB/config/metadb.config"):

    dic_res={}
    
    if(os.path.exists(configFile)==True):
        
        flines= open(configFile)
        #ay_lines=flines.readlines()
        
        line =flines.readline()
        i=0
        
        while(line!=None):
            if(line.replace("\n","")=="# EstLen"):
                ay_count= flines.readline().split(":")
                print ay_count
                
                
                while(i<int(ay_count[1])):
                    ay_new_line= flines.readline().split("=")
                    key= ay_new_line[0].replace("$CONFIG","")
                    key= key.replace("{","")
                    key= key.replace("}","")
                    key= key.replace("'","")
                    key= key.replace(" ","")
                    key= key.replace("\t","")
                        
                    value= ay_new_line[1].replace("'","")
                    value= value.replace(";","")
                    value= value.replace(" ","")
                    value= value.replace("\n","")
                        
                    dic_res[key]=value
                    i+=1
                       
                break
            line=flines.readline()       

    return dic_res
        
#get the maximum id of one table
def getId(conn, fieldid, tablename):

    #debug
    #print conn
    with conn:
        cur = conn.cursor()
        cur.execute("select MAX("+fieldid+") from "+tablename+";")
        res= cur.fetchone()
        return res[0]


# insert record
# sid: species id of the table
# sname: species name
# return the new row id
def insertSpe(conn, sname):
    
    #debug
    print conn
    if(conn!=None):
        with conn:
            cur= conn.cursor()
            cur.execute("select sid from species where sname='"+sname+"';")
            
            rows = cur.fetchall()
            
            if(0==len(rows)):
                cur.execute("insert into species(sname) values('"+sname+"');")
                res= cur.lastrowid
                if(res!=None):
                    print G+ getDateTime()+": New record added species id "+res+" succeed!"+E
                    return res
                else:
                    print R+ getDateTime()+": New record added species id failed!"+E
                    return False
            else:
                print G+ getDateTime()+": Record species id "+str(rows[0][0])+" Found!"+E
                return int(rows[0][0])
            

# insert cultivar
def insertCul(conn, cname, biokey, codedname=""):
    if(conn!=None):
        with conn:
            cur= conn.cursor()
            cur.execute("select cid from cultivar where cname='"+cname+"';")
            
            rows = cur.fetchall()
            
            if(0==len(rows)):
                cur.execute("insert into cultivar(cname, biosource_library_key, codedName) values('"+cname+"','"+biokey+"', '"+codedname+"');")
                res= cur.lastrowid
                if(res!=None):
                    print G+ getDateTime()+": New record added cultivar id "+res+" succeed!"+E
                    return res
                else:
                    print R+ getDateTime()+": New record added cultivar id failed!"+E
                    return False
            else:
                print G+ getDateTime()+": Record cultivar id "+str(rows[0][0])+" Found!"+E
                return int(rows[0][0])

# insert library
def insertLib(conn, libname, sid, cid):
    
    if(conn!=None):
        with conn:
            cur= conn.cursor()
            cur.execute("select lid from library where libname='"+libname+"';")
            
            rows = cur.fetchall()
            
            if(0==len(rows)):
                cur.execute("insert into library(libName, sid, cid) values('"+libname+"',"+str(sid)+","+str(cid)+");")
                res= cur.lastrowid
                if(res!=None):
                    print G+ getDateTime()+": New record added library id "+res+" succeed!"+E
                    return res
                else:
                    print R+ getDateTime()+": New record added library id failed!"+E
                    return False
            else:
                print G+ getDateTime()+": Record library id "+str(rows[0][0])+" Found!"+E
                return int(rows[0][0])
                
# get the real insert size                 
def getRealInsertSize(tmdb, libName):
    
    ay_res=[]
    file_Insert_size= tmdb+"/MetaDB_processing/temp/"+libName+"_insert_size.csv";
    if(os.path.exists(file_Insert_size)==True):
        fp= open(file_Insert_size)
        ay_res= fp.readlines()
    
    return ay_res

# check the results, if the results already have the library name and lane number, return false
def checkExistRes(conn, libName, lane_num):
    
    with conn:
        cur= conn.cursor()
        sql= "select * from results as r where r.lid in "
        sql+="(select lid from library as l where l.libname='"+libName+"') and r.lane_num="+str(lane_num)
        cur.execute(sql)
        if(cur.rowcount!=0):
            print cur.rowcount
            return True
        else:
            print cur.rowcount
            return False
    
    
    
# complete the database
def insertRes(conn, sqlfile, lid, lane_number, issize, real_is, date, readA, readB, cmtspath):
    if(os.path.exists(sqlfile)==False):
        print R+getDateTime()+": Can't find sql file."+E
    else:
        print G+getDateTime()+": The SQL file is "+sqlfile+E
        f=open(sqlfile)
        szsql=f.readline()
        ay_sql=szsql.split(",")

        # read length
        rlength= ay_sql[10].replace(" ", "")
        szRealIS= ",".join(real_is)
        
        # num of reads
        tmp= ay_sql[9].replace(" ", "")
        tmp=str(tmp)
        if(len(tmp)==0 or float(tmp)==0.0):
            nsreads= 0
            npreads= 0
        else:
            npreads=float(tmp)
            nsreads= npreads*2

        # total number of bp
        ntotalbp= int(rlength)*int(nsreads)
        
        # get estimated length
        EstLen= getEstLength()[getBiokey(conn, lid)]
        
        # estimated coverage
        if(EstLen!="" and int(EstLen)!=0):
            estCover= float(ntotalbp)/float(EstLen)
    
        # add comments from cm
        cmtspath=cmtspath+"/cmts.txt"
        comments=""
        if(os.path.exists(cmtspath)!=True):
            print R+getDateTime()+": Comments lost!"+E
        else:
            fc=open(cmtspath)
            comments=fc.read()
            comments=comments.replace("\n","")
            print comments
            fc.close()

        
        if(conn!=None):
            with conn:
                cur= conn.cursor()
                sql="insert into results(lid, lane_num, date, read_length, insert_size, real_insert_size, "
                sql+="nsreads, npreads, ntotalbp, estCover, readA, readB, comments) values ("
                sql+=str(lid)+","
                sql+=str(lane_number)+","
                sql+="'" + date+"',"
                sql+=rlength +","
                sql+=str(issize) +","
                sql+="'" + szRealIS +"',"
                sql+=str(nsreads) +","
                sql+=str(npreads) +","
                sql+=str(ntotalbp) +","
                sql+=str(estCover) +","
                sql+="'" + readA +"',"
                sql+="'" + readB +"',"
                sql+="'"+ comments +"');"

                print cur.execute(sql)
                
                
                if(cur.rowcount!=0):
                    print G+getDateTime()+": Insert result record successfully!"+E
                else:
                    print R+getDateTime()+": Insert result record failed!"+E

# update details of finished job
def updateJobInfo(conn, jobid, startDT, endDT, logPath):
    log=""
    if(os.path.exists(logPath)==True):
        fp= open(logPath)
        log=fp.read()
        fp.close()
        
    if(conn!=None):
        with conn:
            cur= conn.cursor()
            cur.execute("update job set Done=1, StartDateTime='"+startDT+"', EndDateTime='"+endDT+"', log='"+log+"' where jobid="+str(jobid))
            if(cur.rowcount!=0):
                print "Update job record succeed!"
            else:
                print "Update job record failed!"

                
# clean up
def clean(tmpdir):
    shutil.rmtree(tmpdir)
     
# send email
def sendEmail(subject, content, subdir, recvEmail):

    for elm in recvEmail:
        cmd="echo "+content+" | mail -s "+subject+" -t "+elm
        os.system(cmd)

# get the current Date and time
# return a string
def getDateTime():
    return time.strftime("%Y-%m-%d %A %X %Z",time.localtime())


# get the current Date and time and make it into sql format(results table)
def getSQLDateTime():
    return time.strftime("%Y-%m-%d",time.localtime())


# addtional function
# in case of the failed job, run again
# rm files under 
#   /home/miseq01/bin/TurboMetaDB/MetaDB_processing/blast
#   /home/miseq01/bin/TurboMetaDB/MetaDB_processing/fastq
#   /home/miseq01/bin/TurboMetaDB/MetaDB_processing/fasta
def cleanLastRun(TMDBdir):
    
    fastaDir=TMDBdir+"/MetaDB_processing/fasta/"
    blastDBDir= TMDBdir+"/MetaDB_processing/blastdb/"
    #tempDir= TMDB+"/MetaDB_processing/temp/"
    
    for elm_fasta in os.listdir(fastaDir):
        os.remove(fastaDir+elm_fasta)
            
    for elm_blast in os.listdir(blastDBDir):
        os.remove(blastDBDir+elm_blast)
    
    #for elm_temp in os.listdir(tempDir):
    #    os.remove(tempDir+elm_temp)
    
# help instruction
def help():
    print "*"*60
    print "This is TurboMetaDB automatically execution script."
    print "Created at 11/05/2012"
    print "*"*60
    

################################################################################
#   PM takes e.g. python x.py /home/miseq01/rawData/20121205/xxx libraryName email
#   parameter 1: subdir
#   parameter 2: library name
#   parameter 3: email address you want post result to
#   parameter 4: job id [no working for current version]
################################################################################
if(__name__=="__main__"):

    if(4==len(sys.argv)):
        
        """ Main global variables """
        libname=sys.argv[2] # Library name  
        species_name=""     # Species name
        cultivar_name=""    # Cultivar name
        formats="Illumina"  # Format
        iReadslimits= 2000000   # reads limit
        ccurDate=""         # current Date 
        cmain= sys.argv[1]  # subdir folder or rawData folder
        ay_full_path= []    # Full path of raw data, all the subfolders
        userEmail= sys.argv[3]  # user's email address
        start_DateTime=getDateTime()   # job's start date and time
        end_DateTime=""     # job's finish date and time

        # TurboMetaDB base folder
        TMDB="/home/miseq01/bin/TurboMetaDB/"
        PIPELINE= TMDB+"MetaDB/bin/pipeline.pl" 

        # quality png dir
        qudir= TMDB+"/MetaDB_processing/plots/"
        """ End of global variables"""
 
        print G+getDateTime()+": Working directory is "+cmain+E
        
        if(checkUsercsv(cmain+"/user.csv")==False):
            print R+getDateTime()+" Error: Invalid user csv file or could not find user.csv..."+E 
        else:    
            # 1. clean last run(should be end of this for loop)
            print G+getDateTime()+": Clean last job.."+E
            cleanLastRun(TMDB)
                
            # get species name
            speciesname= getSpeciesName(cmain+"/user.csv")            

            #debug
            #print "Species Name: ", speciesname
            
            # get cultiva name
            cultiva_name= getCulName(cmain+"/user.csv")            

            #debug
            #print "Cultivar Name: ", cultiva_name
            
            # 2. check MD5
            print  G+getDateTime()+": MD5 Checking..."+E
                
            if(checkMD5(cmain+"/data.md5", cmain)!=True):
                print R+getDateTime()+" Error: MD5 doesn't match!"+E
            else:
                print G+getDateTime()+": Passed MD5 checking!"+E
                                  
                # split user.csv to run TMDB several times. Expanable to future use
                print G+getDateTime()+": Splitting user.csv..."+E
                csvs= splitCSV(cmain+"/user.csv")
                    
                count=1 #counts for how many times that the TMDB runs
                    
                for elm_uc in csvs:
                    
                    # checking database records
                    lane_num= getLaneNum(elm_uc)

                    if(checkExistRes(conn(), libname, lane_num)==True):
                        print R+getDateTime()+": Results record library name: "+libname+", lane_num "+lane_num+" found, please use a different combination..." +str(count)+E
                        sys.exit()

                    # 3. backup to barrine first
                    ay_rawFiles= getRawFilePath(elm_uc)
                    reads=[]                    
 
                    # make remote folder
                    print G+getDateTime()+": Create remote folder on barrine..."+E
                    makeRemoteFolder(autoSel(), speciesname, cultiva_name, cmain)
                    
                    # uploading raw data files to barrine 
                    for rawfile in ay_rawFiles:
                        """
                        #uploadedfile= upload(autoSel(), rawfile, speciesname, cultiva_name, cmain)
                        uploadedfile=""
                        if(uploadedfile==""):
                            print R+getDateTime()+": Uploading "+rawfile+" Failed!"+E
                        else:
                            print G+getDateTime()+": Uploading "+rawfile+" Done!"+E
                        # check data file intigrity on barrine
                        if(checkBarrineMD5(rawfile, uploadedfile, autoSel())==True):
                            print G+getDateTime()+": Raw Data on barrine passed MD5 check!"+E
                        else:
                            print R+getDateTime()+": Raw Data on barrine Failed MD5 check!"+E
                        """
                        # fileName should be xxx.fastq
                        fileName= makeFastqName(rawfile)
                        print "--fileName: "+fileName
                        reads.append(fileName)
                        print "reads:"
                        print reads
                        time.sleep(10)                       
                      
                    # 4. Fullfill user.csv
                    print "elm_uc:" +elm_uc
                    cmplCSV(elm_uc)                    
     
                    # 5. run TurboMetaDB
                    runTMDB(PIPELINE, elm_uc, libname, formats, iReadslimits)
                                      
                    # make quality names
                    ay_qupngs= makeQuPngNames(TMDB+"/MetaDB_processing/plots/", elm_uc)
                     
                    if(checkExtQuPng(ay_qupngs)==True):
                        print G+getDateTime()+": Creating quality images..."+E
                    else:
                        print R+getDateTime()+": Create quality images Failed!"+E 
                    
                    # draw insert size png
                    r(elm_uc, cmain+"/insertSize.png", cmain)           
                    
                    # collect and upload small files                    
                    gzfile= collect(cmain, ay_qupngs, cmain+"/insertSize.png")
                    print G+getDateTime()+": Compressing images files, csv file, md5 file to "+gzfile+E                       
                    print G+getDateTime()+": Uploading "+gzfile+"..."+E                
                    upload(autoSel(), gzfile, speciesname, cultiva_name, cmain)
                  
                    # 6. insert into mysql
                    sqlfile= TMDB+"/MetaDB_processing/temp/"+libname+"_insert_record.sql"
                    print G+getDateTime()+": Searching "+sqlfile+"..."+E
                    
                    ftmpcsv=open(elm_uc)
                    szcsv= ftmpcsv.readline()
                    szcsv=szcsv.replace("\n","")
                    ay_csv= szcsv.split(",")               
                    print G+getDateTime()+": Get a new array from CSV..."+E                    
                    #print ay_csv
 
                    sid= insertSpe(conn(),ay_csv[6])
                    time.sleep(5)
                    #debug
                    #print "<font color='red'> sid "+str(sid)+"</font><br />"
                    cid= insertCul(conn(),ay_csv[3], ay_csv[2],"")
                    #debug
                    #print "<font color='red'> cid "+str(cid)+"</font><br />"
                    lid= insertLib(conn(),libname, sid, cid)
                    #debug
                    #print "<font color='red'> lid "+str(lid)+"</font><br />"
                    
                    print G+getDateTime()+": Insert a new record into mysql database..."+E
                    
                    ay_RealInsertSize= getRealInsertSize(TMDB, libname)
                    insertRes(conn(), sqlfile, int(lid), int(ay_csv[0]), int(ay_csv[8]), ay_RealInsertSize, getSQLDateTime(), reads[0], reads[1], cmain)
                    print G+getDateTime()+": Insert a new record into mysql database done!"+E

                    # clear tmp dir
                    print G+getDateTime()+": Clean up temporary files..."+E
                    clean(cmain+"/tmp/")
                    #os.remove(cmain+"/tmp/smallfile.tar.gz") 

                    
                    # send confirm email
                    emailAddrs= ["uqyliu19@uq.edu.au"]
                    emailAddrs.append(userEmail)
                    print G+getDateTime()+": Sending email..."+E
                    sendEmail("RAMA_TurboMetaDB_Job_Finished!", "http://safs-rama.mgmt.science.uq.edu.au/checkJobs?done=1", cmain, emailAddrs)
                    
                    count+=1 # if user.csv contains morthan one line, then contintue to run

                makeDone(cmain)
                    
    else:
        print "Must have the right number of parameters.\n"
    
