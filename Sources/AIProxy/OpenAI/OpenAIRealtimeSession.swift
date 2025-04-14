//
//  RealtimeSession.swift
//  ... // Other comments
//

import Foundation
import AVFoundation // Required for audio handling elsewhere, not directly in this func

// Assume RealtimeActor is defined elsewhere, e.g.:
// actor RealtimeActor { }
// typealias RealtimeActor = GlobalActor // Or use a custom actor

// Define the possible messages yielded by the session stream
public enum OpenAIRealtimeMessage: Sendable {
    case sessionCreated
    case sessionUpdated
    case responseAudioDelta(String) // Base64 Encoded Audio Chunk
    case responseCreated
    case inputAudioBufferSpeechStarted
    case transcriptionDelta(String)
    case transcriptionCompleted(String)
    case error(String) // Error description
    // case unknown(String, [String: Any]) // Optional: If you want to yield unknown types
}

@RealtimeActor
open class OpenAIRealtimeSession {

    // --- ADDED: Properties required by the function ---
    private var isTearingDown: Bool = false
    // Assuming the stream yields OpenAIRealtimeMessage or throws an Error
    private var continuation: AsyncThrowingStream<OpenAIRealtimeMessage, Error>.Continuation?
    // You'll need a way to set this continuation when starting the stream, e.g., in an init or connect method

    // --- ADDED: Placeholder methods required by the function ---
    open func disconnect() {
        print("[OpenAIRealtimeSession] disconnect() called.")
        // Actual implementation would:
        // 1. Set isTearingDown = true
        // 2. Close the WebSocket connection
        // 3. Finish the continuation: self.continuation?.finish()
        // 4. Clean up resources
        self.isTearingDown = true // Mark as tearing down
        self.continuation?.finish() // Signal end of stream
        self.continuation = nil // Release continuation
        // ... (add WebSocket closing logic here) ...
    }

    private func receiveMessage() {
        // Check if still connected and not tearing down before scheduling next receive
        guard !isTearingDown /* && webSocket?.isConnected == true */ else {
             print("[OpenAIRealtimeSession] Not receiving next message (tearing down or disconnected).")
             return
        }
        print("[OpenAIRealtimeSession] receiveMessage() called (placeholder - should listen for next WebSocket message).")
        // Actual implementation would likely use the WebSocket library's mechanism
        // to asynchronously wait for the next message, and upon receiving it,
        // call didReceiveWebSocketData(data) again.
        // e.g., webSocket?.receive(completionHandler: { [weak self] result in ... self?.didReceiveWebSocketData(...) })
    }

    // --- Original Function (Modified for basic print logging) ---
    private func didReceiveWebSocketData(_ data: Data) {
        guard !self.isTearingDown else {
            print("[OpenAIRealtimeSession] Ignored WebSocket data - tearing down.")
            return
        }

        guard let json = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
              let messageType = json["type"] as? String else {
            print("[OpenAIRealtimeSession] ERROR: Received websocket data that we don't understand: \(String(data: data, encoding: .utf8) ?? "Non-UTF8 data")")
            // Consider yielding an error instead of just disconnecting?
            // self.continuation?.finish(throwing: MyDecodingError.invalidJSON) // Example
            self.disconnect() // Disconnect on unparseable message
            return
        }
        print("[OpenAIRealtimeSession] DEBUG: Received WebSocket message - Type: \(messageType)") // More specific log

        // --- Use the defined enum cases for yielding ---
        switch messageType {
        case "error":
            let errorBody = String(describing: json["error"] as? [String: Any] ?? json["error"] as? String ?? "Unknown error format")
            print("[OpenAIRealtimeSession] ERROR: Received error from OpenAI websocket: \(errorBody)")
            // Yield our specific error case
            self.continuation?.yield(.error(errorBody))

        case "session.created":
            print("[OpenAIRealtimeSession] DEBUG: Yielding .sessionCreated")
            self.continuation?.yield(.sessionCreated)

        case "session.updated":
             print("[OpenAIRealtimeSession] DEBUG: Yielding .sessionUpdated")
            self.continuation?.yield(.sessionUpdated)

        case "response.audio.delta":
            if let base64Audio = json["delta"] as? String {
                 print("[OpenAIRealtimeSession] DEBUG: Yielding .responseAudioDelta (length: \(base64Audio.count))")
                self.continuation?.yield(.responseAudioDelta(base64Audio))
            } else {
                 print("[OpenAIRealtimeSession] WARNING: Received response.audio.delta event but couldn't extract base64 audio.")
            }

        case "response.created":
             print("[OpenAIRealtimeSession] DEBUG: Yielding .responseCreated")
            self.continuation?.yield(.responseCreated)

        case "input_audio_buffer.speech_started":
             print("[OpenAIRealtimeSession] DEBUG: Yielding .inputAudioBufferSpeechStarted")
            self.continuation?.yield(.inputAudioBufferSpeechStarted)

        // --- BEGIN Transcription Handling ---
        case "conversation.item.input_audio_transcription.delta":
            if let deltaText = json["delta"] as? String {
                print("[OpenAIRealtimeSession] DEBUG: Yielding .transcriptionDelta: \(deltaText)")
                self.continuation?.yield(.transcriptionDelta(deltaText))
            } else {
                print("[OpenAIRealtimeSession] WARNING: Received transcription delta event but couldn't extract 'delta' text. JSON: \(json)")
            }

        case "conversation.item.input_audio_transcription.completed":
             if let completedText = json["transcript"] as? String {
                 print("[OpenAIRealtimeSession] DEBUG: Yielding .transcriptionCompleted: \(completedText)")
                 self.continuation?.yield(.transcriptionCompleted(completedText))
             } else {
                 print("[OpenAIRealtimeSession] WARNING: Received transcription completed event but couldn't extract 'transcript' text. JSON: \(json)")
             }
        // --- END Transcription Handling ---

        default:
             // Log unknown types instead of just breaking silently
             print("[OpenAIRealtimeSession] WARNING: Received unknown message type: \(messageType) - JSON: \(json)")
             // Optionally yield an unknown case:
             // self.continuation?.yield(.unknown(messageType, json))
             break
        }

        // Only continue listening if it wasn't a fatal error (like undecodable JSON which caused an early return)
        // Simple "error" messages from OpenAI itself should still allow listening to continue.
        if !self.isTearingDown { // Check again in case disconnect was called by yield consumer
            self.receiveMessage()
        }
        // If we disconnected due to bad JSON earlier, receiveMessage() won't be called.
    }

    // --- Make sure the class has a closing brace ---
} // End of OpenAIRealtimeSession class
