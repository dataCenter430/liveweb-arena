#!/usr/bin/env python3
"""
Systematic test for all Taostats templates.

Features:
- Tests each question type systematically
- Saves results to JSON after each test (prevents data loss)
- Prints progress information
- Generates summary statistics
"""

import asyncio
import json
import os
import sys
import traceback
from datetime import datetime
from typing import Any, Dict, List

sys.path.insert(0, '.')

from liveweb_arena.plugins.taostats.templates import (
    SubnetInfoTemplate,
    NetworkTemplate,
    PriceTemplate,
    ComparisonTemplate,
    AnalysisTemplate,
)
from liveweb_arena.plugins.taostats.templates.variables import SubnetMetric
from liveweb_arena.plugins.taostats.templates.comparison import ComparisonMetric
from liveweb_arena.plugins.taostats.templates.analysis import AnalysisType
from liveweb_arena.plugins.taostats.templates.network import NetworkMetric


# Output file
RESULTS_FILE = "scripts/test_results.json"


def load_results() -> Dict[str, Any]:
    """Load existing results or create new structure"""
    if os.path.exists(RESULTS_FILE):
        with open(RESULTS_FILE, 'r') as f:
            return json.load(f)
    return {
        "start_time": datetime.now().isoformat(),
        "tests": [],
        "summary": {}
    }


def save_results(results: Dict[str, Any]):
    """Save results to JSON file"""
    results["last_updated"] = datetime.now().isoformat()
    with open(RESULTS_FILE, 'w') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)


def print_progress(current: int, total: int, template: str, metric: str):
    """Print progress information"""
    pct = (current / total) * 100
    print(f"[{current}/{total}] ({pct:.1f}%) Testing: {template} - {metric}")


def serialize_value(val: Any) -> Any:
    """Serialize value to JSON-compatible format"""
    if val is None:
        return None
    if isinstance(val, (str, int, float, bool)):
        return val
    if isinstance(val, (list, tuple)):
        return [serialize_value(v) for v in val]
    if isinstance(val, dict):
        return {str(k): serialize_value(v) for k, v in val.items()}
    if hasattr(val, 'value'):  # Enum
        return val.value
    return str(val)


async def test_single_question(
    template,
    template_name: str,
    metric_name: str,
    seed: int,
) -> Dict[str, Any]:
    """Test a single question and return detailed results"""
    result = {
        "template": template_name,
        "metric": metric_name,
        "seed": seed,
        "timestamp": datetime.now().isoformat(),
    }

    try:
        # Generate question
        q = template.generate(seed)
        result["question"] = q.question_text
        result["start_url"] = q.start_url
        result["validation_info"] = serialize_value(q.validation_info)
        result["variables"] = serialize_value(q.variables)

        # Get ground truth
        gt = await template.get_ground_truth(q.validation_info)

        if gt is None:
            result["ground_truth"] = None
            result["ground_truth_raw"] = None
            result["passed"] = False
            result["error"] = "Ground truth unavailable"
            result["answer_details"] = [{
                "question": q.question_text,
                "expected": None,
                "actual": None,
                "score": 0.0,
                "is_correct": False,
                "reasoning": "Ground truth unavailable from API",
            }]
            return result

        # Format ground truth - save full structure
        result["ground_truth_raw"] = serialize_value(gt)
        if isinstance(gt, tuple):
            result["ground_truth"] = gt[0]
            answer = gt[0]
        else:
            result["ground_truth"] = serialize_value(gt)
            answer = str(gt)

        # Validate with correct answer
        validation = await template.validate_answer(answer, q.validation_info)

        result["score"] = validation.score
        result["is_correct"] = validation.is_correct
        result["expected"] = str(validation.expected) if validation.expected else None
        result["actual"] = str(validation.actual) if validation.actual else None
        result["details"] = validation.details
        result["passed"] = validation.score >= 0.8

        # Build answer_details array (similar to env.py format)
        result["answer_details"] = [{
            "question": q.question_text,
            "expected": str(validation.expected) if validation.expected else None,
            "actual": str(validation.actual) if validation.actual else None,
            "score": validation.score,
            "is_correct": validation.is_correct,
            "reasoning": validation.details,
        }]

        # Build conversation (simulated since no actual agent)
        result["conversation"] = [
            {
                "role": "system",
                "content": "Template validation test - verifying ground truth and scoring logic",
                "metadata": {"type": "test_setup"}
            },
            {
                "role": "user",
                "content": f"Question: {q.question_text}\nStart URL: {q.start_url}",
                "metadata": {"type": "question", "template": template_name, "metric": metric_name}
            },
            {
                "role": "assistant",
                "content": f"Ground truth answer: {answer}",
                "metadata": {"type": "ground_truth_answer", "raw_value": serialize_value(gt)}
            },
            {
                "role": "system",
                "content": f"Validation result: score={validation.score}, is_correct={validation.is_correct}, details={validation.details}",
                "metadata": {"type": "validation_result"}
            }
        ]

    except Exception as e:
        result["passed"] = False
        result["error"] = f"{type(e).__name__}: {str(e)}"
        result["traceback"] = traceback.format_exc()
        result["answer_details"] = [{
            "question": result.get("question", "Unknown"),
            "expected": None,
            "actual": None,
            "score": 0.0,
            "is_correct": False,
            "reasoning": f"Exception: {str(e)}",
        }]
        result["conversation"] = [
            {
                "role": "system",
                "content": f"Test failed with exception: {str(e)}",
                "metadata": {"type": "error", "traceback": traceback.format_exc()}
            }
        ]

    return result


def find_seed_for_metric(template, metric_value: str, start_seed: int = 1000) -> int:
    """Find a seed that generates a question with the specified metric"""
    for seed in range(start_seed, start_seed + 200):
        q = template.generate(seed)
        if q.validation_info.get("metric") == metric_value:
            return seed
        if q.validation_info.get("analysis_type") == metric_value:
            return seed
    return start_seed


async def run_all_tests():
    """Run all tests systematically"""
    results = load_results()

    # Define all test cases
    test_cases = []

    # 1. SubnetInfoTemplate - all metrics
    for metric in SubnetMetric:
        test_cases.append(("SubnetInfoTemplate", metric.value, SubnetInfoTemplate()))

    # 2. NetworkTemplate - all metrics
    for metric in NetworkMetric:
        test_cases.append(("NetworkTemplate", metric.value, NetworkTemplate()))

    # 3. PriceTemplate
    test_cases.append(("PriceTemplate", "tao_price", PriceTemplate()))

    # 4. ComparisonTemplate - all metrics
    for metric in ComparisonMetric:
        test_cases.append(("ComparisonTemplate", metric.value, ComparisonTemplate()))

    # 5. AnalysisTemplate - all types
    for atype in AnalysisType:
        test_cases.append(("AnalysisTemplate", atype.value, AnalysisTemplate()))

    total = len(test_cases)
    passed_count = 0
    failed_count = 0

    print("="*70)
    print("TAOSTATS TEMPLATE SYSTEM TEST")
    print("="*70)
    print(f"Total tests: {total}")
    print(f"Results file: {RESULTS_FILE}")
    print("="*70)

    for i, (template_name, metric_name, template) in enumerate(test_cases, 1):
        print_progress(i, total, template_name, metric_name)

        # Find appropriate seed for this metric
        if template_name == "SubnetInfoTemplate":
            seed = find_seed_for_metric(template, metric_name, 1000 + i * 100)
        elif template_name == "NetworkTemplate":
            seed = find_seed_for_metric(template, metric_name, 2000 + i * 100)
        elif template_name == "ComparisonTemplate":
            seed = find_seed_for_metric(template, metric_name, 3000 + i * 100)
        elif template_name == "AnalysisTemplate":
            seed = find_seed_for_metric(template, metric_name, 4000 + i * 100)
        else:
            seed = 1000 + i

        # Run test
        result = await test_single_question(template, template_name, metric_name, seed)

        # Update counts
        if result.get("passed", False):
            passed_count += 1
            status = "PASS"
        else:
            failed_count += 1
            status = "FAIL"

        # Print result
        gt = result.get("ground_truth", "N/A")
        if gt and len(str(gt)) > 40:
            gt = str(gt)[:40] + "..."
        error = result.get("error", "")
        print(f"    [{status}] GT: {gt} | {error}")

        # Add to results and save
        results["tests"].append(result)
        results["summary"] = {
            "total": i,
            "passed": passed_count,
            "failed": failed_count,
            "pass_rate": f"{(passed_count/i)*100:.1f}%"
        }
        save_results(results)

    # Final summary
    print("\n" + "="*70)
    print("TEST COMPLETE")
    print("="*70)
    print(f"Total: {total}")
    print(f"Passed: {passed_count}")
    print(f"Failed: {failed_count}")
    print(f"Pass rate: {(passed_count/total)*100:.1f}%")
    print(f"\nResults saved to: {RESULTS_FILE}")

    # Print failed tests
    if failed_count > 0:
        print("\nFailed tests:")
        for test in results["tests"]:
            if not test.get("passed", False):
                print(f"  - {test['template']}/{test['metric']}: {test.get('error', test.get('details', 'unknown'))}")

    print("="*70)

    return failed_count == 0


if __name__ == "__main__":
    success = asyncio.run(run_all_tests())
    sys.exit(0 if success else 1)
