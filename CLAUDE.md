# Claude Code Memory

## Development Guidelines

1. **Occam's Razor** - Keep code minimal while maintaining quality
2. **Engineering First** - Every change should improve overall project structure
3. **Zero Redundancy** - No redundant code allowed
4. **Fix Root Cause** - Never patch over problems, always solve at the root
5. **Test Driven** - Every bug fix must have a corresponding test case
6. **File Size** - Keep files under 500 lines
7. **Import Style** - Use absolute imports for cross-package (`liveweb_arena.core.xxx`), relative for same package
8. **Commit Rules** - Only commit when explicitly asked; keep messages concise
9. **Template Testing** - Every new question template must be tested via `eval.py` with multiple seeds to verify the entire evaluation pipeline works correctly

## Template Design Guidelines

1. **Parameterization over Enumeration**
   - Use Variable classes with large pools (30+ items)
   - Never hardcode specific entity names in question patterns
   - Seed-based sampling ensures reproducibility without memorization

2. **Real-time Data Dependency**
   - Questions must require precise data that LLMs cannot reliably recall
   - Prefer runtime, exact dates, specific credits over general knowledge
   - Avoid yes/no questions about well-known facts

3. **Verifiability Chain**
   - Every question must have: Template -> API endpoint -> Ground truth
   - API response must match website display (same source)
   - Validation tolerance must account for format differences only

4. **Solvability Guarantee**
   - Target website must be publicly accessible
   - Required information must be visible without authentication
   - Expected interaction steps should be realistic (5-15 for single questions)

5. **Difficulty Stratification**
   - Easy: Single-hop, direct URL navigation, one data point
   - Medium: Search required, or multiple data points from same page
   - Hard: Multi-page navigation, comparison, or aggregation
