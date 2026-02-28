import os
import argparse
from datetime import datetime
from google import genai
from google.genai import types
from dotenv import load_dotenv
from zoneinfo import ZoneInfo

# Load environment variables
load_dotenv()

# Configure LLM
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
if not GEMINI_API_KEY:
    GEMINI_API_KEY = os.getenv("GOOGLE_API_KEY")

if GEMINI_API_KEY:
    client = genai.Client(api_key=GEMINI_API_KEY)
else:
    client = None
    print("Warning: GEMINI_API_KEY or GOOGLE_API_KEY not found in .env. AI features will fail.")

# ─────────────────────────────────────────────────────────────────────────────
# AI Synthesis Logic
# ─────────────────────────────────────────────────────────────────────────────

def synthesize_analysis(ticker, model="gemini-2.5-flash"):
    """Use Gemini to generate a fully grounded 4-part investment report with comparative data."""
    if not client:
        return "Error: Gemini API key not configured or client initialization failed."
    
    current_date = datetime.now(ZoneInfo("America/Chicago")).strftime("%Y-%m-%d")
    
    system_instruction = """
    You are a world-class institutional investor (think Warren Buffett meets a modern quant-fundamentalist).
    Your task: Provide a HIGHLY CONCISE, LATEST, DATA-DRIVEN deep-dive analysis of the requested stock ticker.

    ### CRITICAL BEHAVIORAL RULES:
    1. **MANDATORY**: YOU MUST USE THE GOOGLE SEARCH TOOL to find the absolute latest earnings reports, guidance, news, Google Trends data.
    2. **Data Source**: ONLY use official company guidance (forward-looking statements). DO NOT use market expectations.
    3. **Grounding**: All claims must be grounded in NUMBERS (revenue, margins, growth rates, market share).
    4. **Math Accuracy**: Use your Python Code Execution tool to calculate all % changes to ensure mathematical accuracy.
    5. **Conciseness**: Follow a strictly point-form style. Be extremely concise but cover all required sections.
    6. **Citations**: Include numbers naturally in the text and rely on the Search tool to back them up. YOU MUST ensure EVERY section has citations.
    7. **Quality Control**: For news, strictly exclude content farm or AI-generated articles.
    8. **Missing Data Handling**: If specific segment revenues, YoY guidance metrics, are genuinely not available/disclosed, explicitly state 'Not disclosed' or 'No recent significant discussions found' rather than hallucinating or using third-party estimates.
    """

    prompt = f"""
    Today's Date: {current_date}
    Analyze ticker: {ticker}

    ### SECTION-SPECIFIC RULES (WHAT YOU MUST GENERATE):
    1. **Business Overview & Segments**: Provide a 1-2 sentence summary of what the company does. List the formal business segments. Provide the most recently reported quarterly/annual revenue breakdown by segment (including % YoY growth for each). Then, provide the revenue split by geographic region with YoY growth. Explicitly state "Region data not available" if it is undisclosed.
    2. **Management Guidance & YoY Comparison**: Look up the explicitly issued forward-looking guidance from the company's latest earnings report. Provide the specific ranges (e.g., Revenue, EPS, Margins). You MUST calculate and display the % change or bps change compared to the exact same metric from a) the same quarter and b) the year prior. NEVER use analyst or market estimates here. If not disclosed, explicitly state 'Not disclosed'.
    3. **Management Expectations**: Summarize the 3-4 most critical qualitative forward-looking statements made by the CEO/CFO on the latest earnings call. Focus on things like AI traction, enterprise expansion, product adoption rates, or long-term financial targets.
    4. **Recent Important News**: Summarize 3 of the most recent, high-impact news catalysts (e.g., new products, order backlog changes, executive changes, major deals). Exclude noise and AI content farms.
    5. **Moat**: Define {ticker}'s own structural advantages. Provide valid, durable moats backed by data. DO NOT use cliché business jargon like "first-mover advantage" or "economies of scale" unless mathematically proven.
    6. **Risks**: List the top 3 objective risks to the business model or stock price.
    7. **Competitor Analysis**: Explicitly state {ticker}'s estimated market share and provide a key quantitative metric to size it (e.g., Annual Revenue). Then, list its top 3-5 direct competitors. For each competitor, provide a detailed profile including: estimated market share, their latest annual/quarterly revenue with YoY growth %, their core target audience, their specific competitive moat, and their primary strengths and weaknesses relative to {ticker} (using hard numbers where possible).
    8. **Google Trends**: Search to confirm the stock's exchange first if unknown, then execute a Search tool call specifically for `"EXCHANGE: {ticker}"` to find trend comparisons against peers. State {ticker}'s relative interest level (e.g., Difference % vs peers) and summarize the historical trend of search volume over the past 3 months vs the past 12 months.
    9. **Drivers Behind Price Movement**: State the recent price movement (up/down X% over Y timeframe) and dissect the primary financial or macroeconomic catalysts causing the movement (e.g., a massive earnings beat, sector rotation, or a product launch).

    ### STYLE REFERENCE (FOLLOW THIS EXACT FORMAT AND ORDERING):
    ---
    ### Business Overview & Segments
    Digital Ocean (DOCN) is a cloud platform provider for developers and small tech firms.
    
    **Business segments:**
    - **Compute**: VMs (Droplets), GPUs
    - **Storage**: Object storage (Spaces), backups
    - **Databases**: Managed database services

    **Revenue & Growth by Segment:**
    - **Compute**: VMs (Droplets), GPUs - $XXXM (+X% YoY)
    - **Storage & Databases**: Object storage (Spaces), Managed DBs - $XXXM (+X% YoY)

    **Revenue & Growth by Region:** (Strictly only show if data is available. If not, state that data is not available.)
    - North America $XXXM (+X% YoY), International $XXXM (+X% YoY)

    ### Management Guidance & YoY Comparison
    **Q4 2025 Guidance (issued Q3 2025)** (ONLY USE COMPANY GUIDANCE, NOT MARKET EXPECTATIONS)
    - **Revenue**: $237M–$238M (vs. Q4 2024 actual $205M → **+15.6% to +16.1% YoY**)
    - **Adjusted EBITDA margin**: 38.5%–39.5% (vs. Q4 2024 actual 42% → **down 250–350 bps**)
    - **Non-GAAP EPS**: $0.35–$0.40 (vs. Q4 2024 actual $0.49 → **down ~18% to ~29%**)
    
    **Full-Year 2025 Guidance (Raised)** (ONLY USE COMPANY GUIDANCE, NOT MARKET EXPECTATIONS)
    - **Revenue**: $896M–$897M (vs. FY 2024 actual $781M → **+14.7% to +14.9% YoY**)
    - **FCF margin**: 18%–19% (vs. FY 2024 actual 17% → **up 100–200 bps**)
    
    ### Management Expectations 
    - **Strong AI Traction**: AI customer ARR surged 150% YoY to $120M in Q4 2025.
    - **Enterprise Expansion**: Million-dollar customers generated $133M in ARR in Q4 2025, up 123% YoY.
    - **Long-term Outlook**: Raised 2027 revenue outlook, aiming for 25%+ growth by end of 2026.

    ### Recent Important News
    - **Focus**: [Insert strictly meaningful, high-quality recent news items. Skip if none.]

    ### Moat
    - **Valid Moat**: [Describe genuine structural advantages, e.g., 'Proprietary InP fab with 1700 bps higher leverage', or 'High switching costs due to proprietary hyperscaler ecosystem lock-in']. DO NOT use business cliches like "economies of scale" or "first-mover advantage" unless backed by hard data.

    ### Risks
    - **Competition risk**: hyperscalers and other cloud providers can outspend.
    - **Cyber/security risk**: incidents or outages could damage trust, trigger liability, and hurt retention.
    - **Execution risk (AI/product)**: failure to ship competitive features / AI offerings fast enough could weaken positioning.

    ### Competitor Analysis
    - **DOCN Sizing/Share**: DOCN holds ~X% of the SMB cloud market, running at a ~$900M revenue run-rate.
    - **AWS (AMZN)**: 
      - **Market Share & Revenue**: ~X% ($XXXB Cloud Revenue, +X% YoY)
      - **Target Audience**: Large enterprises & broad dev community.
      - **Moat**: Massive global footprint and ecosystem lock-in.
      - **Comparison vs DOCN**: AWS outspends DOCN $30B+ in capex annually, but their bandwidth egress fees are up to 10x higher than DOCN, isolating cost-sensitive SMBs.
    - **Azure (MSFT)**: 
      - **Market Share & Revenue**: ~X% ($XXXB Cloud Revenue, +X% YoY)
      - **Target Audience**: Enterprise B2B.
      - **Moat**: Seamless integration with existing Microsoft enterprise software (Office 365, Windows).
      - **Comparison vs DOCN**: Azure has the enterprise locked down, but lacks DOCN's transparent pricing and developer-first simplicity.

    ### Google Trends Search Interest
    - **Search Criteria**: MUST search using `"NYSE: {ticker}"` or `"NASDAQ: {ticker}"` (choose the correct exchange) to compare vs. peers.
    - **Vs Peers**: Searches for "{ticker}" vs main competitors. State the absolute level (exact number) and relative level of interest vs peers (e.g., Difference %).
    - **Historical**: Search interest trends over the past 3 months vs 12 months.

    ### Drivers Behind Recent Price Movement
    - **Price Movement**: [Insert whether stock is up/down recently and timeframe (last week, last month, last 3 months and last 12 months)].
    - **Main Reasons**: [Insert catalysts such as earnings beats, macro factors, or product launches].
    ---

    NOW ANALYZE {ticker} FOLLOWING THE INSTRUCTIONS AND STYLE ABOVE.
    """
    
    try:
        # Properly configure tools using the types module
        config = types.GenerateContentConfig(
            system_instruction=system_instruction,
            tools=[
                types.Tool(google_search=types.GoogleSearch()),
                types.Tool(code_execution=types.ToolCodeExecution())
            ],
            thinking_config=types.ThinkingConfig(
                include_thoughts=False,
                thinking_budget=10000
            )
        )

        response = client.models.generate_content(
            model=model,
            contents=prompt,
            config=config
        )
        def add_citations(response):
            candidate = response.candidates[0]
            # Safely grab the full text
            text_parts = [p.text for p in candidate.content.parts if hasattr(p, 'text') and p.text]
            text = "".join(text_parts)
            
            metadata = getattr(candidate, 'grounding_metadata', None)
            
            if not metadata:
                print("No grounding_metadata found")    
                return text
                
            supports = getattr(metadata, 'grounding_supports', []) or []
            chunks = getattr(metadata, 'grounding_chunks', []) or []

            if supports and chunks:
                insertions = {}
                for support in supports:
                    seg_text = getattr(support.segment, 'text', "")
                    if not seg_text or not support.grounding_chunk_indices:
                        continue
                        
                    # Safely find the EXACT index in the Python string (avoids Unicode offset bugs)
                    idx = text.find(seg_text)
                    if idx == -1:
                        idx = text.find(seg_text.strip())
                        actual_len = len(seg_text.strip())
                    else:
                        actual_len = len(seg_text)
                        
                    if idx != -1:
                        end_index = idx + actual_len
                        
                        # Create citation string safely
                        for i in support.grounding_chunk_indices:
                            if i < len(chunks):
                                # Safe extraction to prevent AttributeErrors
                                web = getattr(chunks[i], 'web', None)
                                uri = getattr(web, 'uri', '') if web else ''
                                
                                if uri:
                                    cite_str = f" [[{i + 1}]({uri})]"
                                else:
                                    cite_str = f" [{i + 1}]"
                                    
                                if end_index not in insertions:
                                    insertions[end_index] = []
                                if cite_str not in insertions[end_index]:
                                    insertions[end_index].append(cite_str)

                # Apply citations in reverse order so string manipulation doesn't shift earlier indices
                for end_idx in sorted(insertions.keys(), reverse=True):
                    citation_string = "".join(insertions[end_idx])
                    text = text[:end_idx] + citation_string + text[end_idx:]

            # Append Sources list
            if chunks:
                sources_list = []
                for i, chunk in enumerate(chunks):
                    web = getattr(chunk, 'web', None)
                    if web and hasattr(web, 'uri'):
                        title = getattr(web, 'title', f"Source {i+1}")
                        uri = getattr(web, 'uri', "")
                        if uri:
                            sources_list.append(f"- [{i + 1}] {title}: {uri}")
                
                if sources_list:
                    text += "\n\n---\n**Sources:**\n" + "\n".join(sources_list)

            return text

        # Return grounded result
        return add_citations(response)
    except Exception as e:
        return f"Error generating analysis: {str(e)}"

# ─────────────────────────────────────────────────────────────────────────────
# Main Runner
# ─────────────────────────────────────────────────────────────────────────────

def main():
    """
    USAGE: uv run python ai_analyzer.py --ticker SNDK 
    """
    parser = argparse.ArgumentParser(description="Deep-Dive AI Stock Analyzer")
    parser.add_argument("--ticker", type=str, required=True, help="Stock ticker (e.g., TSLA, NVDA)")
    args = parser.parse_args()
    
    ticker = args.ticker.upper()
    print(f"--- Deep-Dive Analysis for {ticker} (Fully Search-Grounded) ---")
    
    # Switched to standard flash for highly reliable tool usage
    model = "gemini-3.1-pro-preview" 
    print(f"[1/1] Researching and synthesizing investment report with AI ({model})...")
    report = synthesize_analysis(ticker, model)
    
    print("\n" + "="*80)
    print(report)
    print("="*80 + "\n")

if __name__ == "__main__":
    main()

