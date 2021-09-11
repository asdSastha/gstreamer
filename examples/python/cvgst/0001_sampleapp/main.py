import os
import sys

import cv2
import numpy as np

import gi
gi.require_version('Gst', '1.0')
from gi.repository import Gst, GObject

def bus_call(bus, message, loop):
    t = message.type
    if t == Gst.MessageType.EOS:
        sys.stdout.write("End-of-stream\n")
        loop.quit()
    elif t == Gst.MessageType.ERROR:
        err, debug = message.parse_error()
        sys.stderr.write("Error: %s: %s\n" % (err, debug))
        loop.quit()
    return True

def main():
    
    capture = cv2.VideoCapture("./couple.mp4")
    while(True):
        status, frame = capture.read()
        if status:
            result = cv2.Canny(frame, 10, 100, 3, 3)
            cv2.imshow('Output', result)
            
            if (cv2.waitKey(1) & 0xFF == ord('q')):
                break
        else:
            break
    
    print("=== Running Gstreamer Pipeline ===")
    GObject.threads_init()
    Gst.init(None)
    
    loop = GObject.MainLoop()
    pipeline = Gst.parse_launch("uridecodebin uri=file:"+os.getcwd()+"/couple.mp4 ! autovideosink")
    
    # Registering the bus
    bus = pipeline.get_bus()
    bus.add_signal_watch()
    bus.connect ("message", bus_call, loop)
    
    # Setting properties of appsrc
    pipeline.set_state(Gst.State.PLAYING)
    try:
        loop.run()
    except:
        pass

    print("ALL IS WELL")
    return


if __name__ == "__main__":
    main()
