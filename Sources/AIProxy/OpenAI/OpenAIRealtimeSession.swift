//
//  RealtimeSession.swift
//  ... // Other comments
//

import Foundation // <--- ADD THIS LINE
import AVFoundation // This should already be there

@RealtimeActor
open class OpenAIRealtimeSession {

// Modified function to handle transcription events
    private func didReceiveWebSocketData(_ data: Data) {
        guard !self.isTearingDown else {
            // The caller already initiated disconnect,
            // don't send any more messages back to the caller
            return
        }

        guard let json = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
              let messageType = json["type"] as? String else {
            logIf(.error)?.error("Received websocket data that we don't understand: \(String(data: data, encoding: .utf8) ?? "Non-UTF8 data")")
            // Consider yielding an error instead of just disconnecting?
            // self.continuation?.yield(.error("Undecodable JSON received"))
            self.disconnect()
            return
        }
        logIf(.debug)?.debug("Received WebSocket message - Type: \(messageType)") // More specific log

        switch messageType {
        case "error":
            let errorBody = String(describing: json["error"] as? [String: Any])
            logIf(.error)?.error("Received error from OpenAI websocket: \(errorBody)")
            self.continuation?.yield(.error(errorBody))

        case "session.created":
            logIf(.debug)?.debug("Yielding .sessionCreated")
            self.continuation?.yield(.sessionCreated)

        case "session.updated":
             logIf(.debug)?.debug("Yielding .sessionUpdated")
            self.continuation?.yield(.sessionUpdated)

        case "response.audio.delta":
            if let base64Audio = json["delta"] as? String {
                 logIf(.debug)?.debug("Yielding .responseAudioDelta (length: \(base64Audio.count))")
                self.continuation?.yield(.responseAudioDelta(base64Audio))
            } else {
                 logIf(.warning)?.warning("Received response.audio.delta event but couldn't extract base64 audio.")
            }

        case "response.created":
             logIf(.debug)?.debug("Yielding .responseCreated")
            self.continuation?.yield(.responseCreated)

        case "input_audio_buffer.speech_started":
             logIf(.debug)?.debug("Yielding .inputAudioBufferSpeechStarted")
            self.continuation?.yield(.inputAudioBufferSpeechStarted)

        // --- BEGIN Transcription Handling ---
        case "conversation.item.input_audio_transcription.delta":
            if let deltaText = json["delta"] as? String {
                logIf(.debug)?.debug("Yielding .transcriptionDelta: \(deltaText)")
                self.continuation?.yield(.transcriptionDelta(deltaText))
            } else {
                logIf(.warning)?.warning("Received transcription delta event but couldn't extract 'delta' text. JSON: \(json)")
            }

        case "conversation.item.input_audio_transcription.completed":
             if let completedText = json["transcript"] as? String {
                 logIf(.debug)?.debug("Yielding .transcriptionCompleted: \(completedText)")
                 self.continuation?.yield(.transcriptionCompleted(completedText))
             } else {
                 logIf(.warning)?.warning("Received transcription completed event but couldn't extract 'transcript' text. JSON: \(json)")
             }
        // --- END Transcription Handling ---

        default:
             // Log unknown types instead of just breaking silently
             logIf(.warning)?.warning("Received unknown message type: \(messageType) - JSON: \(json)")
             // Optionally, you could add an `.unknown(String, [String: Any])` case to OpenAIRealtimeMessage
             // and yield it here if you want the client code to be aware of unknown events.
             break
        }

        // Only continue listening if it wasn't a fatal error (like undecodable JSON)
        // Simple "error" messages from OpenAI itself should still allow listening to continue.
        if messageType != "error" && !self.isTearingDown {
            self.receiveMessage()
        } else if messageType == "error" && !self.isTearingDown {
            // If it was an OpenAI domain error, still try to receive the next message
             self.receiveMessage()
        }
        // If we disconnected due to bad JSON earlier, receiveMessage() won't be called.
    }
