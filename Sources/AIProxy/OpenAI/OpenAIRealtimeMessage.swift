//
//  OpenAIRealtimeMessage.swift
//  AIProxy
//
//  Created by Lou Zell on 12/29/24.
//

import Foundation // It's good practice to import Foundation even if not strictly needed by the enum itself

/// Represents messages received from the OpenAI Realtime WebSocket API.
public enum OpenAIRealtimeMessage {
    /// An error occurred, potentially containing a descriptive message.
    case error(String?)

    /// The session was successfully created.
    case sessionCreated // "session.created"

    /// The session configuration was updated.
    case sessionUpdated // "session.updated"

    /// A response from the assistant has been initiated.
    case responseCreated // "response.created"

    /// A chunk of audio data representing the assistant's speech.
    case responseAudioDelta(String) // "response.audio.delta", payload is base64 encoded audio

    /// VAD detected that the user started speaking.
    case inputAudioBufferSpeechStarted // "input_audio_buffer.speech_started"

    /// An incremental update to the user's speech transcription.
    /// Expected type: "conversation.item.input_audio_transcription.delta", field: "delta"
    case transcriptionDelta(String)

    /// The final transcription for a completed speech segment.
    /// Expected type: "conversation.item.input_audio_transcription.completed", field: "transcript"
    case transcriptionCompleted(String)

    // Note: The library might need cases for other events like:
    // - input_audio_buffer.speech_stopped
    // - conversation.item.text.delta
    // - conversation.item.tool_call.delta
    // - etc., depending on the full Realtime API spec and desired features.
}
