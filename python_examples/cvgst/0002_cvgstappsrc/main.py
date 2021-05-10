import os
import sys

import numpy as np
import cv2
import time
from fractions import Fraction

import gi
gi.require_version('Gst', '1.0')
gi.require_version('GstVideo', '1.0')
from gi.repository import Gst, GObject, GstVideo
import gstreamer.utils as utils

VIDEO_FORMAT = "RGB"
WIDTH, HEIGHT = 640, 480
GST_VIDEO_FORMAT = GstVideo.VideoFormat.from_string(VIDEO_FORMAT)
FPS = Fraction(30) # Initially limiting to 30 fps

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

    GObject.threads_init()
    Gst.init(None)
    loop = GObject.MainLoop()
    #Todo: add caps

    pipeline = Gst.parse_launch(
        "appsrc emit-signals=True is-live=True name=gstappsrc caps=video/x-raw,format=BGR,width=640,height=480,framerate=30/1  ! queue ! videoconvert ! autovideosink"
    )
    appsrc = pipeline.get_by_name('gstappsrc')
    appsrc.set_property("format", Gst.Format.TIME)
    appsrc.set_property("block", True)  # Blocks adding elements to queus so that heavy allocations wont happen
    
    try:
        capture = cv2.VideoCapture("./couple.mp4")
        pts = 0
        duration =  10**9 / (FPS.numerator / FPS.denominator)  # frame duration
        pipeline.set_state(Gst.State.PLAYING)
        while (True):
             
            # get numpy array from cv video stream
            status, frame = capture.read()
            if status:
                frame = cv2.resize(frame, (WIDTH, HEIGHT)) 
                gst_buffer = utils.ndarray_to_gst_buffer(frame)
                # set pts and duration to be able to record video, calculate fps
                pts += duration  # Increase pts by duration
                gst_buffer.pts = pts
                gst_buffer.duration = duration
                # emit <push-buffer> event with Gst.Buffer
                appsrc.emit("push-buffer", gst_buffer)
            else:
                appsrc.emit("end-of-stream")
                break
        
    except Exception as e:
        print("Error: ", e)
    finally:
        print("All is well !!!")


if __name__ == "__main__":
    main()