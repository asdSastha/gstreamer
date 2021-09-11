import sys
import typing as typ
from fractions import Fraction

import numpy as np
import cv2

from gstreamer import GObject, GstContext, GstPipeline, GstApp, Gst, GstVideo
import gstreamer.utils as utils

#Configuration
WIDTH, HEIGHT, CHANNELS = 640, 480, 3
INPUT_VIDEO_FILE = "./couple.mp4"
RESULT_FILE = 'result.avi'
FPS = Fraction(60)


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


result = cv2.VideoWriter(RESULT_FILE, cv2.VideoWriter_fourcc(*'mp4v'), 1,
                         (WIDTH, HEIGHT))
capture = cv2.VideoCapture(INPUT_VIDEO_FILE)
pts = 0
duration = (10**9 / (FPS.numerator / FPS.denominator)) / 2


def push_frame(appsrc, length):
    global pts
    try:
        status, frame = capture.read()
        if status:
            frame = cv2.resize(frame, (WIDTH, HEIGHT))
            gst_buffer = utils.ndarray_to_gst_buffer(frame)
            pts += duration
            gst_buffer.pts = pts
            gst_buffer.duration = duration
            # emit <push-buffer> event with Gst.Buffer
            appsrc.emit("push-buffer", gst_buffer)
        else:
            appsrc.emit("end-of-stream")
    except Exception as e:
        print("Error: ", e)


def stop_buffer(self, src):
    #Todo free the resources as per requirement
    result.release()
    print("End of stream !!")


def extract_buffer(sample: Gst.Sample) -> np.ndarray:
    """Extracts Gst.Buffer from Gst.Sample and converts to np.ndarray"""

    buffer = sample.get_buffer()  # Gst.Buffer

    caps_format = sample.get_caps().get_structure(0)  # Gst.Structure

    # GstVideo.VideoFormat
    video_format = GstVideo.VideoFormat.from_string(
        caps_format.get_value('format'))

    # w, h = caps_format.get_value('width'), caps_format.get_value('height')
    # c = utils.get_num_channels(video_format)

    buffer_size = buffer.get_size()
    format_info = GstVideo.VideoFormat.get_info(
        video_format)  # GstVideo.VideoFormatInfo
    array = np.ndarray(shape=buffer_size //
                       (format_info.bits // utils.BITS_PER_BYTE),
                       buffer=buffer.extract_dup(0, buffer_size),
                       dtype=utils.get_np_dtype(video_format))
    array = array.reshape(HEIGHT, WIDTH, CHANNELS).squeeze()
    return np.squeeze(array)  # remove single dimension if exists


def on_buffer(sink: GstApp.AppSink, data: typ.Any) -> Gst.FlowReturn:
    """Callback on 'new-sample' signal"""
    # Emit 'pull-sample' signal
    # https://lazka.github.io/pgi-docs/GstApp-1.0/classes/AppSink.html#GstApp.AppSink.signals.pull_sample

    sample = sink.emit("pull-sample")  # Gst.Sample

    if isinstance(sample, Gst.Sample):
        array = extract_buffer(sample)
        print("Received {type} with shape {shape} of type {dtype}".format(
            type=type(array), shape=array.shape, dtype=array.dtype))
        result.write(array)
        return Gst.FlowReturn.OK

    return Gst.FlowReturn.ERROR


def main():

    GObject.threads_init()
    Gst.init(None)
    loop = GObject.MainLoop()
    command = "appsrc emit-signals=True is-live=True name=gstappsrc caps=video/x-raw,format=BGR,width=640,height=480,framerate=100/1  ! queue max-size-buffers=4 ! videoconvert ! appsink emit-signals=True name=gstappsink"
    pipeline = Gst.parse_launch(command)

    # Registering the bus
    bus = pipeline.get_bus()
    bus.add_signal_watch()
    bus.connect("message", bus_call, loop)

    ## Setting properties of appsrc
    appsrc = pipeline.get_by_name('gstappsrc')
    appsrc.set_property("format", Gst.Format.TIME)
    appsrc.connect('need-data', push_frame)
    appsrc.connect('enough-data', stop_buffer)
    # Blocks adding elements to queus so that heavy allocations wont happen
    appsrc.set_property("block", True)

    # Setting properties of appsink
    appsink = pipeline.get_by_name('gstappsink')
    appsink.connect("new-sample", on_buffer, None)
    pipeline.set_state(Gst.State.PLAYING)

    try:
        loop.run()
    except Exception as e:
        print("Error: ", e)
    finally:
        print("All is Well !!!")


if __name__ == "__main__":
    main()