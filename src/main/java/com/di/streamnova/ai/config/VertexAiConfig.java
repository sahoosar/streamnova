package com.di.streamnova.ai.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.client.advisor.MessageChatMemoryAdvisor;
import org.springframework.ai.chat.memory.InMemoryChatMemory;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.embedding.EmbeddingModel;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;

/**
 * Spring AI configuration for Vertex AI / Gemini.
 *
 * <p>Auto-configuration from {@code spring-ai-vertex-ai-gemini-spring-boot-starter}
 * creates a {@link ChatModel} and {@link EmbeddingModel} bean. This class wraps them
 * into higher-level components used by the AI agents.
 *
 * <p>All tunable parameters live in {@code application.yml} → {@code spring.ai.vertex.ai.gemini.*}
 * — no code changes required for model/parameter swaps.
 *
 * <p>Disabled entirely when {@code streamnova.ai.enabled=false}.
 */
@Slf4j
@Configuration
@RequiredArgsConstructor
@ConditionalOnProperty(name = "streamnova.ai.enabled", havingValue = "true", matchIfMissing = true)
public class VertexAiConfig {

    private final AiProperties aiProperties;

    /**
     * System prompt loaded from {@code classpath:prompts/system-prompt.st}.
     * Swap the file to change the assistant personality without touching code.
     */
    @Value("classpath:prompts/system-prompt.st")
    private Resource systemPromptResource;

    /**
     * General-purpose {@link ChatClient} with in-memory conversation history.
     *
     * <p>The {@link MessageChatMemoryAdvisor} prepends the last N turns of the
     * conversation so Gemini maintains context across multiple API calls.
     */
    @Bean
    public ChatClient chatClient(ChatModel chatModel) {
        log.info("[AI-CONFIG] Initialising ChatClient → model={}, project={}, location={}",
                aiProperties.getChat().getModel(),
                aiProperties.getVertex().getProjectId(),
                aiProperties.getVertex().getLocation());

        InMemoryChatMemory memory = new InMemoryChatMemory();
        int maxTurns = aiProperties.getAgent().getMemoryMaxTurns();

        return ChatClient.builder(chatModel)
                .defaultSystem(systemPromptResource)
                .defaultAdvisors(
                        MessageChatMemoryAdvisor.builder(memory)
                                .conversationId("default")
                                .build()
                )
                .build();
    }

    /**
     * Stateless {@link ChatClient} — no conversation history; used by tools that
     * make one-shot classification / extraction calls inside pipelines.
     */
    @Bean(name = "statelessChatClient")
    public ChatClient statelessChatClient(ChatModel chatModel) {
        return ChatClient.builder(chatModel).build();
    }
}
