import sys
import traceback
import argparse
import typing as typ
import time
import attr

import numpy as np
import cv2

from gstreamer import GObject, GstContext, GstPipeline, GstApp, Gst, GstVideo
import gstreamer.utils as utils

WIDTH, HEIGHT, CHANNELS = 640, 480, 3


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


result = cv2.VideoWriter('video.avi', cv2.VideoWriter_fourcc(*'mp4v'), 1,
                         (WIDTH, HEIGHT))


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
    command = "videotestsrc  num-buffers=100 ! capsfilter caps=video/x-raw,format=RGB,width=640,height=480 ! appsink emit-signals=True name=gstappsink"
    pipeline = Gst.parse_launch(command)

    # Registering the bus
    bus = pipeline.get_bus()
    bus.add_signal_watch()
    bus.connect("message", bus_call, loop)

    # Setting properties of appsrc
    appsink = pipeline.get_by_name('gstappsink')
    appsink.connect("new-sample", on_buffer, None)

    pipeline.set_state(Gst.State.PLAYING)
    try:
        loop.run()
    except:
        pass
    result.release()
    print("All is well!!")


if __name__ == "__main__":
    main()