import asyncio
from autogen_core import DefaultTopicId, RoutedAgent, message_handler, type_subscription, MessageContext
from api.data_types import AgentEnum, AgentRequest, AgentStructuredResponse, MarketResearch, MarketResearchResult, APIKeys, ErrorResponse
from utils.logging import logger
from utils.error_utils import format_api_error_message
from services.market_research_service import MarketResearchService

@type_subscription(topic_type="market_research")
class MarketResearchAgent(RoutedAgent):
    """Agent that performs market research using Exa via MarketResearchService."""

    def __init__(self, api_keys: APIKeys):
        super().__init__("MarketResearchAgent")
        logger.info(logger.format_message(None, f"Initializing MarketResearchAgent with ID: {self.id}"))
        self.api_keys = api_keys
        self.service = MarketResearchService()
        self.service.api_key = api_keys.exa_key

    @message_handler
    async def handle_market_research_request(self, message: AgentRequest, ctx: MessageContext) -> None:
        try:
            logger.info(logger.format_message(ctx.topic_id.source, "Processing market research request"))
            params = message.parameters
            summary = await asyncio.to_thread(
                self.service.generate_market_research,
                industry=params.industry,
                product=params.product,
            )
            result = MarketResearchResult(summary=summary)
            response = AgentStructuredResponse(
                agent_type=self.id.type,
                data=result,
                message=message.parameters.model_dump_json(),
                message_id=message.message_id,
            )
            await self.publish_message(
                response,
                DefaultTopicId(type="user_proxy", source=ctx.topic_id.source),
            )
        except Exception as e:
            logger.error(logger.format_message(ctx.topic_id.source, f"Error processing market research request: {str(e)}"), exc_info=True)
            error_response = format_api_error_message(e, "market research")
            response = AgentStructuredResponse(
                agent_type=AgentEnum.Error,
                data=ErrorResponse(error=error_response),
                message=f"Error processing market research request: {str(e)}",
                message_id=message.message_id,
            )
            await self.publish_message(
                response,
                DefaultTopicId(type="user_proxy", source=ctx.topic_id.source),
            )
