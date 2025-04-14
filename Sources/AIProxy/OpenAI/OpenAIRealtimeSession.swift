//
//  RealtimeSession.swift
//  AIProxy
//
//  Created by Lou Zell on 11/28/24.
//

import Foundation // <-- Ensure this is present
import AVFoundation
import Network // Add import for NWPOSIXErrorDomain

@RealtimeActor
open class OpenAIRealtimeSession {
    private var isTearingDown = false
    private let webSocketTask: URLSessionWebSocketTask
    private var continuation: AsyncStream<OpenAIRealtimeMessage>.Continuation?
    let sessionConfiguration: OpenAIRealtimeSessionConfiguration // Assuming this is initialized elsewhere

    // Assuming an initializer exists like this (or similar)
    init(
        webSocketTask: URLSessionWebSocketTask,
        sessionConfiguration: OpenAIRealtimeSessionConfiguration
    ) {
        self.webSocketTask = webSocketTask
        self.sessionConfiguration = sessionConfiguration

        // Initial setup is moved to handle session.created message
        // Task {
        //      // Example: Send initial configuration
        //      await self.sendMessage(OpenAIRealtimeSessionUpdate(session: self.sessionConfiguration))
        // }
        self.webSocketTask.resume()
        self.receiveMessage()
    }

    deinit {
        logIf(AIProxyLogLevel.debug)?.debug("OpenAIRealtimeSession is being freed")
        // Ensure teardown logic if not already handled elsewhere
        if !isTearingDown {
            Task { await disconnect() }
        }
    }

    /// Messages sent from OpenAI are published on this receiver as they arrive
    public var receiver: AsyncStream<OpenAIRealtimeMessage> {
        return AsyncStream { continuation in
            // Should handle potential multiple assignments carefully if receiver is accessed multiple times
            self.continuation = continuation
            // Optional: Handle stream termination
            continuation.onTermination = { @Sendable [weak self] _ in
                // Clean up if the stream is terminated externally
                 logIf(AIProxyLogLevel.debug)?.debug("OpenAIRealtimeSession receiver terminated.")
                 Task { await self?.disconnect() }
            }
        }
    }

    /// Sends a message through the websocket connection
    public func sendMessage(_ encodable: Encodable) async {
        guard webSocketTask.closeCode == .invalid else {
            logIf(AIProxyLogLevel.warning)?.warning("Attempted to send message on a closed WebSocket.")
            return
        }
        guard !self.isTearingDown else {
            logIf(AIProxyLogLevel.debug)?.debug("Ignoring ws sendMessage. The RT session is tearing down.")
            return
        }
        do {
            // Assuming existence of a suitable serialize method (e.g., on Encodable extension)
            let jsonEncoder = JSONEncoder()
            // Configure encoder if needed (e.g., key strategies)
            let jsonData = try jsonEncoder.encode(encodable)
            guard let jsonString = String(data: jsonData, encoding: .utf8) else {
                 logIf(AIProxyLogLevel.error)?.error("Failed to encode message to UTF8 string.")
                 return
            }
            let wsMessage = URLSessionWebSocketTask.Message.string(jsonString)

            logIf(AIProxyLogLevel.debug)?.debug("Sending WebSocket message: \(jsonString)") // Log outgoing message
            try await self.webSocketTask.send(wsMessage)
        } catch {
            logIf(AIProxyLogLevel.error)?.error("Could not send message to OpenAI: \(error.localizedDescription)")
            // Consider yielding an error to the continuation or handling reconnection
        }
    }

    /// Close the websocket connection
    public func disconnect() async {
         guard !isTearingDown else { return }
         isTearingDown = true
         logIf(AIProxyLogLevel.debug)?.debug("Disconnecting from realtime session")
         webSocketTask.cancel(with: .goingAway, reason: nil)
         continuation?.finish() // Signal end of stream
         continuation = nil
    }


    // Keep trying to receive messages
    private func receiveMessage() {
        guard !isTearingDown && webSocketTask.closeCode == .invalid else {
            logIf(AIProxyLogLevel.debug)?.debug("Not receiving message, session tearing down or closed.")
            return
        }

        webSocketTask.receive { [weak self] result in
            guard let self = self else { return }

            switch result {
            case .success(let message):
                switch message {
                case .string(let text):
                     logIf(AIProxyLogLevel.debug)?.trace("Received WebSocket string: \(text)")
                    if let data = text.data(using: .utf8) {
                        self.didReceiveWebSocketData(data)
                    } else {
                        logIf(AIProxyLogLevel.error)?.error("Failed to convert received WebSocket string to data.")
                        // Decide how to handle this error (e.g., disconnect, yield error)
                         self.receiveMessage() // Try to receive next message
                    }
                case .data(let data):
                     logIf(AIProxyLogLevel.debug)?.trace("Received WebSocket data: \(data.count) bytes")
                    self.didReceiveWebSocketData(data)
                @unknown default:
                    logIf(AIProxyLogLevel.warning)?.warning("Received unknown WebSocket message type")
                    self.receiveMessage() // Try to receive next message
                }

            case .failure(let error):
                 // Handle potential closures and errors
                 guard !self.isTearingDown else {
                     logIf(AIProxyLogLevel.debug)?.debug("WebSocket receive error during teardown: \(error.localizedDescription)")
                     return // Expected error during disconnection
                 }

                let nsError = error as NSError
                // Ignore "Socket is not connected" (code 57) or POSIX "Connection reset by peer" (code 54) which often happen on disconnect/network change
                if !(nsError.domain == NSPOSIXErrorDomain && (nsError.code == 54 || nsError.code == 57 || nsError.code == 60)) {
                     logIf(AIProxyLogLevel.error)?.error("WebSocket receive error: \(error.localizedDescription)")
                     self.continuation?.yield(.error("WebSocket receive error: \(error.localizedDescription)"))
                } else {
                     logIf(AIProxyLogLevel.debug)?.debug("WebSocket receive error (likely disconnect/timeout): \(error.localizedDescription)")
                }
                // Consider attempting reconnect or finalize teardown
                Task { await self.disconnect() } // Disconnect on receive error
            }
        }
    }


    // Modified function to handle transcription events
    private func didReceiveWebSocketData(_ data: Data) {
        guard !self.isTearingDown else {
            // The caller already initiated disconnect, don't send any more messages
            return
        }

        guard let json = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
              let messageType = json["type"] as? String else {
            logIf(AIProxyLogLevel.error)?.error("Received websocket data that we don't understand: \(String(data: data, encoding: .utf8) ?? "Non-UTF8 data")")
            // Don't disconnect immediately, but maybe yield an error? Or just log.
            // Attempt to receive the next message in case this was a one-off issue.
             if !self.isTearingDown { self.receiveMessage() }
            return
        }
        logIf(AIProxyLogLevel.debug)?.debug("Received WebSocket message - Type: \(messageType)")

        var messageHandled = true // Assume handled unless default case hit without specific handling

        switch messageType {
        case "error":
            let errorBody = String(describing: json["error"] as? [String: Any])
            logIf(AIProxyLogLevel.error)?.error("Received error event from OpenAI websocket: \(errorBody ?? "No details")")
            self.continuation?.yield(.error(errorBody))
            // Note: OpenAI might send 'error' and then close, or expect client to close.
            // We will still call receiveMessage() below to catch potential subsequent messages or closure.

        case "session.created":
            logIf(AIProxyLogLevel.debug)?.debug("Yielding .sessionCreated and sending configuration update...")
            Task { await self.sendMessage(OpenAIRealtimeSessionUpdate(session: self.sessionConfiguration)) }
            self.continuation?.yield(.sessionCreated)

        case "session.updated":
             logIf(AIProxyLogLevel.debug)?.debug("Yielding .sessionUpdated")
            self.continuation?.yield(.sessionUpdated)

        case "response.audio.delta":
            if let base64Audio = json["delta"] as? String {
                 logIf(AIProxyLogLevel.debug)?.debug("Yielding .responseAudioDelta (length: \(base64Audio.count))")
                self.continuation?.yield(.responseAudioDelta(base64Audio))
            } else {
                 logIf(AIProxyLogLevel.warning)?.warning("Received response.audio.delta event but couldn't extract 'delta' field. JSON: \(json)")
            }

        case "response.created":
             logIf(AIProxyLogLevel.debug)?.debug("Yielding .responseCreated")
            self.continuation?.yield(.responseCreated)

        case "input_audio_buffer.speech_started":
             logIf(AIProxyLogLevel.debug)?.debug("Yielding .inputAudioBufferSpeechStarted")
            self.continuation?.yield(.inputAudioBufferSpeechStarted)

        // --- BEGIN Transcription Handling ---
        case "conversation.item.input_audio_transcription.delta":
            // Example structure: {"type": "...", "item_id": "...", "content_index": 0, "delta": "hello "}
            if let deltaText = json["delta"] as? String {
                logIf(AIProxyLogLevel.debug)?.debug("Yielding .transcriptionDelta: \(deltaText)")
                self.continuation?.yield(.transcriptionDelta(deltaText))
            } else {
                logIf(AIProxyLogLevel.warning)?.warning("Received transcription delta event but couldn't extract 'delta' text. JSON: \(json)")
            }

        case "conversation.item.input_audio_transcription.completed":
            // Example structure: {"type": "...", "item_id": "...", "content_index": 0, "transcript": "hello world"}
             if let completedText = json["transcript"] as? String {
                 logIf(AIProxyLogLevel.debug)?.debug("Yielding .transcriptionCompleted: \(completedText)")
                 self.continuation?.yield(.transcriptionCompleted(completedText))
             } else {
                 logIf(AIProxyLogLevel.warning)?.warning("Received transcription completed event but couldn't extract 'transcript' text. JSON: \(json)")
             }
        // --- END Transcription Handling ---

        // TODO: Add cases for other events from OpenAI Realtime API documentation
        // e.g., input_audio_buffer.speech_stopped, conversation.item.text.delta, etc.

        default:
             messageHandled = false // Mark as unhandled
             logIf(AIProxyLogLevel.warning)?.warning("Received unknown message type: \(messageType) - JSON: \(json)")
             // No break here, allow receiveMessage to be called below
        }

        // Always try to receive the next message unless tearing down
        if !self.isTearingDown {
            self.receiveMessage()
        }
    }
    
    // Add any other necessary properties or methods the class originally had.
    // For example, the `logIf` helper needs to be defined or imported.
    // Assuming a simple logger utility:
    // func logIf(_ level: LogLevel) -> Logger? { /* Implementation */ }
    // enum LogLevel { case trace, debug, warning, error }
    // protocol Logger { func trace(...), func debug(...), func warning(...), func error(...) }

    // You'll also need the definition for OpenAIRealtimeSessionUpdate if it's used internally
    // struct OpenAIRealtimeSessionUpdate: Encodable { let session: OpenAIRealtimeSessionConfiguration }
}

// Add other necessary supporting types/extensions that were originally in this file or module.
