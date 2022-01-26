from threading import Timer, Thread, Event
from math import ceil

def calculateInterval(ini, fin):
   #return round(((interval+(fin-ini))/2), 1)+0.1 if interval != -1 else interval+0.1
   return (float(ceil((fin-ini) * 10)) / 10.0) + 0.2

class PerpetualTimer():
   """
   A Thread that executes infinitely
   """
   def __init__(self, hFunction, args, interval=None):
      self.interval = interval
      self.tLastMsg = -1
      self.modifyInterval = True
      self.hFunction = hFunction
      self.args = args
      self.thread = None
      self.setTimer()
      
   def handle_function(self):
      self.hFunction(*self.args)
      self.modifyInterval = False
      self.setTimer()
      self.thread.start()
      
   def start(self):
      self.setTimer()
      if self.thread != None: self.thread.start()
   
   def cancel(self):
      if self.thread != None: self.thread.cancel()
   
   def reset(self, tMsg):
      print(self.modifyInterval)
      if(self.modifyInterval == True):
         print("tiempo 2: " + str(tMsg - self.tLastMsg))
         #if self.tLastMsg != -1: self.interval = calculateInterval(self.interval, self.tLastMsg, tMsg)
         if self.tLastMsg != -1: self.interval = calculateInterval(self.tLastMsg, tMsg)
      else:
         self.modifyInterval = True
      self.tLastMsg = tMsg
      print("interval: " + str(self.interval))
      self.cancel()
      self.start()

   def getInterval(self):
      return self.interval

   def setTimer(self):
      if self.interval != None:
         self.thread = Timer(self.interval, self.handle_function)
         self.thread.daemon = True

