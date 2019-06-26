#import os

#myCmd = os.popen('/usr/local/hadoop/bin/hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.1.2.jar wordcount input grep_example3').read()
#print(myCmd)

import subprocess

class HJob():

    cla='wordcount'
    arg=''
    input='input'
    output='out11'

    def __init__(self,cla='wordcount',input='/home/oussama3m/hadoop_0/data/input/',output='out2w2'):
        self.cla=cla
        self.input=input
        self.output=output

    def exec(self):
        
        MyOut = subprocess.Popen(['/usr/local/hadoop/bin/hadoop','jar','/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.1.2.jar',self.cla,self.input,self.output],stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
        stdout,stderr = MyOut.communicate()
        print(stdout)
        print(stderr)
        coutput=''+self.output+'/part-r-00000'
        print("\n\n")
    
        My2Out = subprocess.Popen(['cat',coutput],stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
        stdout,stderr = My2Out.communicate()
        print(stderr)
        return stdout.decode("utf-8")

