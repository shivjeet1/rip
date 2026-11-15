from __future__ import annotations

from datetime import datetime
from enum import Enum

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class RootCauseCategory(str, Enum):
    DATA_SKEW = "DATA_SKEW"
    SCHEMA_DRIFT = "SCHEMA_DRIFT"
    PARTIAL_PAYOUT_WRITE = "PARTIAL_PAYOUT_WRITE"
    OTHER = "OTHER"


class RetrySafety(str, Enum):
    SAFE_TO_RETRY = "SAFE_TO_RETRY"
    MANUAL_CLEANUP_REQUIRED = "MANUAL_CLEANUP_REQUIRED"


class AgentDiagnosis(BaseModel):
    model_config = ConfigDict(extra="forbid")

    root_cause_category: RootCauseCategory
    summary: str = Field(min_length=20, max_length=4000)
    insurance_business_impact: str = Field(min_length=20, max_length=4000)
    payout_exposure_usd: float = Field(ge=0)
    code_diff: str = Field(min_length=20)
    retry_safety: RetrySafety
    retry_recommendation: str = Field(min_length=20, max_length=6000)

    @field_validator("code_diff")
    @classmethod
    def validate_code_diff(cls, value: str) -> str:
        lowered = value.lower()
        forbidden = (
            "sc." + "parallelize",
            "." + "rdd",
            "." + "_jdf",
            "spark" + "context",
        )
        if any(token in lowered for token in forbidden):
            raise ValueError("code_diff contains a forbidden RDD or JVM API")
        if "---" not in value or "+++" not in value:
            raise ValueError("code_diff must be a unified diff with file headers")
        return value

    @model_validator(mode="after")
    def validate_cleanup_guardrail(self) -> AgentDiagnosis:
        if self.retry_safety == RetrySafety.MANUAL_CLEANUP_REQUIRED:
            if "DELETE FROM" not in self.retry_recommendation.upper():
                raise ValueError("MANUAL_CLEANUP_REQUIRED requires an explicit DELETE FROM statement")
        return self


class DiagnosisRecord(AgentDiagnosis):
    incident_id: str = Field(min_length=3)
    model_used: str = Field(min_length=3)
    created_at: datetime
