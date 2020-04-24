//
// Created by Rahul Acharya on 2/25/20.
//
#pragma  once
enum class MsgKind {
        Ack,
        Get,
        Kill,
        Register,
        Directory,
        Send,
        Finished
};