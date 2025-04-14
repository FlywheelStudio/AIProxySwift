//
//  OpenAIRealtimeMessage.swift
//  AIProxy
//
//  Created by Lou Zell on 12/29/24.
//

public enum OpenAIRealtimeMessage {
    case error(String?)
    case sessionCreated // "session.created"
    case sessionUpdated // "session.updated"
    case responseCreated // "response.created"
    case responseAudioDelta(String) // "response.audio.delta"
    case inputAudioBufferSpeechStarted // "input_audio_buffer.speech_started"
    case transcriptionDelta(String)      // type: conversation.item.input_audio_transcription.delta, field: delta
    case transcriptionCompleted(String)  // type: conversation.item.input_audio_transcription.completed, field: transcript
}
