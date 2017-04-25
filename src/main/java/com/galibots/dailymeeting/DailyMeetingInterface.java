package com.galibots.dailymeeting;

import io.netty.channel.ChannelHandlerContext;

public interface DailyMeetingInterface {
   String handleMessage(ChannelHandlerContext ctx, String frameText);
}
