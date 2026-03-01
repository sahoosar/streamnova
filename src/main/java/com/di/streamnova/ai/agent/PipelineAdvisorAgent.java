package com.di.streamnova.ai.agent;

import com.di.streamnova.ai.config.AiProperties;
import com.di.streamnova.ai.dto.AiChatRequest;
import com.di.streamnova.ai.dto.AiChatResponse;
import com.di.streamnova.ai.tool.DataQualityTool;
import com.di.streamnova.ai.tool.PipelineExecutionTool;
import com.di.streamnova.ai.tool.ProfilerTool;
import com.di.streamnova.ai.tool.RecommendationTool;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.client.advisor.SimpleLoggerAdvisor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.io.Resource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.ResponseBodyEmitter;

import java.util.UUID;

/**
 * Primary AI agent for StreamNova — a fully autonomous Gemini-powered pipeline advisor.
 *
 * <p>This agent receives a natural-language request and orchestrates tool calls to:
 * <ol>
 *   <li>Profile the source table ({@link ProfilerTool})</li>
 *   <li>Check data quality ({@link DataQualityTool})</li>
 *   <li>Generate machine-type recommendations ({@link RecommendationTool})</li>
 *   <li>Optionally execute the pipeline ({@link PipelineExecutionTool})</li>
 * </ol>
 *
 * <p>Gemini decides autonomously which tools to call based on the user's request.
 * No if/else routing code is needed here — the model handles planning.
 *
 * <h3>Adding new capabilities</h3>
 * <p>Just create a new {@code @Tool}-annotated class and inject it here — Gemini
 * will discover and use it without any changes to this agent.
 */
@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name = "streamnova.ai.enabled", havingValue = "true", matchIfMissing = true)
public class PipelineAdvisorAgent {

    private final ChatClient             chatClient;
    private final AiProperties           aiProperties;

    // Tools that Gemini can call
    private final RecommendationTool     recommendationTool;
    private final PipelineExecutionTool  pipelineExecutionTool;
    private final ProfilerTool           profilerTool;
    private final DataQualityTool        dataQualityTool;

    @Value("classpath:prompts/pipeline-advisor-system.st")
    private Resource systemPromptResource;

    /**
     * Processes a user request with full agentic tool-use loop.
     *
     * <p>Gemini will iterate up to {@code ai.agent.max-iterations} times,
     * calling tools as needed, until it produces a final answer.
     *
     * @param request natural-language user request
     * @return agent response with answer and metadata
     */
    public AiChatResponse chat(AiChatRequest request) {
        String conversationId = request.getConversationId() != null
                ? request.getConversationId()
                : "adv-" + UUID.randomUUID();

        log.info("[ADVISOR] conversationId={} user={}", conversationId,
                request.getMessage().length() > 80
                        ? request.getMessage().substring(0, 80) + "…"
                        : request.getMessage());

        long start = System.currentTimeMillis();

        try {
            String answer = chatClient.prompt()
                    .system(systemPromptResource)
                    .user(request.getMessage())
                    // Register all tools — Gemini will choose which ones to call
                    .tools(
                            recommendationTool,
                            pipelineExecutionTool,
                            profilerTool,
                            dataQualityTool
                    )
                    .advisors(new SimpleLoggerAdvisor())
                    .call()
                    .content();

            long durationMs = System.currentTimeMillis() - start;
            log.info("[ADVISOR] Completed conversationId={} durationMs={}", conversationId, durationMs);

            return AiChatResponse.builder()
                    .conversationId(conversationId)
                    .answer(answer)
                    .agentName("PipelineAdvisorAgent")
                    .model(aiProperties.getChat().getModel())
                    .durationMs(durationMs)
                    .success(true)
                    .build();

        } catch (Exception ex) {
            long durationMs = System.currentTimeMillis() - start;
            log.error("[ADVISOR] Failed conversationId={}: {}", conversationId, ex.getMessage(), ex);
            return AiChatResponse.builder()
                    .conversationId(conversationId)
                    .answer("I encountered an error: " + ex.getMessage())
                    .agentName("PipelineAdvisorAgent")
                    .model(aiProperties.getChat().getModel())
                    .durationMs(durationMs)
                    .success(false)
                    .errorMessage(ex.getMessage())
                    .build();
        }
    }

    /**
     * Streaming variant — returns token-by-token for UI streaming responses.
     */
    public ResponseBodyEmitter streamChat(AiChatRequest request) {
        var emitter = new ResponseBodyEmitter(
                (long) aiProperties.getAgent().getTimeoutSeconds() * 1000);

        chatClient.prompt()
                .system(systemPromptResource)
                .user(request.getMessage())
                .tools(recommendationTool, pipelineExecutionTool, profilerTool, dataQualityTool)
                .stream()
                .content()
                .subscribe(
                        token -> {
                            try { emitter.send(token); } catch (Exception e) { emitter.completeWithError(e); }
                        },
                        emitter::completeWithError,
                        emitter::complete
                );
        return emitter;
    }
}
