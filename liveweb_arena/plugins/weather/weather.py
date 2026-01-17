"""Weather plugin using wttr.in with extensible template framework"""

import random
from typing import Dict, List, Type

from liveweb_arena.plugins.base import BasePlugin, SubTask, ValidationResult
from liveweb_arena.core.validators.base import QuestionTemplate, GeneratedQuestion, get_registered_templates

# Import templates to trigger registration via decorators
from . import templates as _  # noqa: F401


class WeatherPlugin(BasePlugin):
    """
    Weather plugin using wttr.in.

    Templates are auto-registered via @register_template decorator:
    - location_name: Single day, single metric queries
    - multi_day: Multi-day aggregate or daily value queries
    - time_of_day: Time-period specific queries (morning/afternoon/evening/night)
    """

    def __init__(self, templates: List[str] = None, use_chinese: bool = False):
        self.use_chinese = use_chinese
        self._template_instances: Dict[str, QuestionTemplate] = {}

        # Get templates from global registry
        registered = get_registered_templates()
        template_names = templates or list(registered.keys())
        for name in template_names:
            if name in registered:
                cls = registered[name]
                # Some templates support use_chinese, others don't
                try:
                    self._template_instances[name] = cls(use_chinese=use_chinese)
                except TypeError:
                    self._template_instances[name] = cls()

    @property
    def name(self) -> str:
        return "weather"

    @property
    def supported_sites(self) -> List[str]:
        return ["wttr.in"]

    @property
    def description(self) -> str:
        return "Query weather information for any location worldwide using wttr.in"

    @property
    def usage_hint(self) -> str:
        return """## Weather Tool (wttr.in)

**Website**: https://wttr.in

**URL Patterns**:
- By city name: https://wttr.in/London or https://wttr.in/New+York
- By airport code: https://wttr.in/JFK
- By coordinates: https://wttr.in/48.8567,2.3508
- By landmark: https://wttr.in/~Eiffel+Tower

**Page Content**:
The page displays an ASCII art weather report showing:
- Current conditions at the top (temperature, wind, visibility)
- 3-day forecast in a table format with Morning/Noon/Evening/Night columns
- Temperature format: +25(28) °C means 25°C actual, 28°C feels-like
- Each day shows high temperatures around Noon/Evening

**Tips**:
- The temperature shown with parentheses like +28(31) means actual temp is 28°C
- Look at the Noon and Evening columns for daily high temperatures
- Rain probability shown as percentage like "0.1 mm | 81%"
"""

    async def generate_task(
        self,
        seed: int,
        template_name: str = None,
        metric: str = None,
    ) -> SubTask:
        """Generate a weather query task using templates"""
        rng = random.Random(seed)

        # Select a template
        if template_name and template_name in self._template_instances:
            selected_template_name = template_name
        else:
            selected_template_name = rng.choice(list(self._template_instances.keys()))
        template = self._template_instances[selected_template_name]

        # Generate question using template
        question: GeneratedQuestion = template.generate(seed)

        # Convert to SubTask (no start_url - Agent decides navigation)
        return SubTask(
            plugin_name=self.name,
            intent=question.question_text,
            validation_info={
                "template_name": selected_template_name,
                **question.validation_info,
            },
            answer_tag="",  # Will be set by TaskManager
        )

    async def validate_answer(
        self,
        answer: str,
        validation_info: dict
    ) -> ValidationResult:
        """Validate answer using the appropriate template"""
        template_name = validation_info.get("template_name", "location_name")
        template = self._template_instances.get(template_name)

        if template is None:
            # Fallback to first available template
            template = list(self._template_instances.values())[0]

        result = await template.validate_answer(answer, validation_info)

        # Convert template ValidationResult to plugin ValidationResult
        return ValidationResult(
            score=result.score,
            is_correct=result.is_correct,
            expected=result.expected,
            actual=result.actual,
            details=result.details,
        )

    async def get_ground_truth(self, validation_info: dict):
        """Get ground truth from the appropriate template"""
        template_name = validation_info.get("template_name", "location_name")
        template = self._template_instances.get(template_name)

        if template is None:
            template = list(self._template_instances.values())[0]

        return await template.get_ground_truth(validation_info)

    def get_validation_rules(self, validation_info: dict) -> str:
        """Get validation rules from the appropriate template"""
        template_name = validation_info.get("template_name", "location_name")
        template = self._template_instances.get(template_name)

        if template is None:
            template = list(self._template_instances.values())[0]

        return template.get_validation_rules(validation_info)

    def register_template(self, name: str, template_class: Type[QuestionTemplate]):
        """
        Register a new question template at runtime.

        For new templates, prefer using the @register_template decorator instead.
        This method is for dynamic registration at runtime.
        """
        try:
            self._template_instances[name] = template_class(use_chinese=self.use_chinese)
        except TypeError:
            self._template_instances[name] = template_class()
