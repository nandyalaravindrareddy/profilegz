import pandas as pd
from datetime import datetime
from typing import Dict, List
import json

class ComplianceReporter:
    def generate_compliance_report(self, profiling_results: Dict, project_id: str) -> Dict:
        """Generate comprehensive compliance report"""
        
        report = {
            "metadata": {
                "project_id": project_id,
                "generated_at": datetime.now().isoformat(),
                "report_type": "Data Compliance & Quality Assessment"
            },
            "executive_summary": self._generate_executive_summary(profiling_results),
            "risk_assessment": self._generate_risk_assessment(profiling_results),
            "compliance_violations": self._identify_compliance_violations(profiling_results),
            "recommendations": self._generate_recommendations(profiling_results),
            "detailed_findings": self._generate_detailed_findings(profiling_results)
        }
        
        return report
    
    def _generate_executive_summary(self, results: Dict) -> Dict:
        """Generate executive summary"""
        result_table = results.get("result_table", {})
        
        total_columns = len(result_table)
        sensitive_columns = len([c for c in result_table.values() if c.get('dlp_info_types')])
        high_risk_columns = len([c for c in result_table.values() if self._is_high_risk(c)])
        
        return {
            "total_columns_analyzed": total_columns,
            "sensitive_data_columns": sensitive_columns,
            "high_risk_columns": high_risk_columns,
            "compliance_score": self._calculate_compliance_score(result_table),
            "overall_risk_level": self._calculate_risk_level(sensitive_columns, total_columns)
        }
    
    def _generate_risk_assessment(self, results: Dict) -> List[Dict]:
        """Generate risk assessment matrix"""
        risks = []
        result_table = results.get("result_table", {})
        
        for col_name, col_data in result_table.items():
            risk_score = self._calculate_column_risk(col_data)
            if risk_score > 0.3:  # Only include medium/high risk
                risks.append({
                    "column": col_name,
                    "risk_score": risk_score,
                    "risk_level": self._get_risk_level(risk_score),
                    "issues": self._get_column_issues(col_data),
                    "recommendation": self._get_column_recommendation(col_data)
                })
        
        return sorted(risks, key=lambda x: x["risk_score"], reverse=True)
    
    def _identify_compliance_violations(self, results: Dict) -> List[Dict]:
        """Identify potential compliance violations"""
        violations = []
        result_table = results.get("result_table", {})
        
        # GDPR violations
        gdpr_sensitive = ["EMAIL_ADDRESS", "PERSON_NAME", "DATE_OF_BIRTH", "IP_ADDRESS"]
        for col_name, col_data in result_table.items():
            dlp_types = col_data.get('dlp_info_types', [])
            gdpr_violations = [t for t in dlp_types if any(gdpr in t for gdpr in gdpr_sensitive)]
            if gdpr_violations:
                violations.append({
                    "regulation": "GDPR",
                    "column": col_name,
                    "violation_types": gdpr_violations,
                    "severity": "High",
                    "description": "Personal data processing without explicit consent mechanisms"
                })
        
        # PCI-DSS violations
        pci_sensitive = ["CREDIT_CARD_NUMBER", "BANK_ACCOUNT_NUMBER"]
        for col_name, col_data in result_table.items():
            dlp_types = col_data.get('dlp_info_types', [])
            pci_violations = [t for t in dlp_types if any(pci in t for pci in pci_sensitive)]
            if pci_violations:
                violations.append({
                    "regulation": "PCI-DSS", 
                    "column": col_name,
                    "violation_types": pci_violations,
                    "severity": "Critical",
                    "description": "Payment card data stored without encryption/tokenization"
                })
        
        return violations
    
    def _generate_recommendations(self, results: Dict) -> List[Dict]:
        """Generate actionable recommendations"""
        recommendations = []
        result_table = results.get("result_table", {})
        
        # Data masking recommendations
        sensitive_cols = [col for col, data in result_table.items() if data.get('dlp_info_types')]
        if sensitive_cols:
            recommendations.append({
                "priority": "High",
                "category": "Data Protection",
                "action": "Implement data masking for sensitive columns",
                "columns": sensitive_cols[:5],
                "business_impact": "Reduce compliance risk and data breach exposure"
            })
        
        # Data quality recommendations
        high_null_cols = [col for col, data in result_table.items() 
                         if data.get('stats', {}).get('null_pct', 0) > 0.5]
        if high_null_cols:
            recommendations.append({
                "priority": "Medium",
                "category": "Data Quality",
                "action": "Address data completeness issues",
                "columns": high_null_cols[:3],
                "business_impact": "Improve analytics accuracy and decision making"
            })
        
        return recommendations
    
    def _calculate_column_risk(self, col_data: Dict) -> float:
        """Calculate risk score for a column (0-1)"""
        risk_score = 0
        
        # DLP findings contribute 60%
        if col_data.get('dlp_info_types'):
            risk_score += 0.6
        
        # High null percentage contributes 20%
        if col_data.get('stats', {}).get('null_pct', 0) > 0.3:
            risk_score += 0.2
        
        # Uniqueness (potential PII) contributes 20%
        if col_data.get('stats', {}).get('distinct_pct', 0) == 1:
            risk_score += 0.2
        
        return min(risk_score, 1.0)
    
    def _is_high_risk(self, col_data: Dict) -> bool:
        return self._calculate_column_risk(col_data) > 0.7
    
    def _calculate_compliance_score(self, result_table: Dict) -> float:
        total_columns = len(result_table)
        if total_columns == 0:
            return 100
        
        compliant_columns = len([c for c in result_table.values() if not c.get('dlp_info_types')])
        return (compliant_columns / total_columns) * 100
    
    def _calculate_risk_level(self, sensitive_cols: int, total_cols: int) -> str:
        ratio = sensitive_cols / total_cols if total_cols > 0 else 0
        if ratio > 0.3: return "Critical"
        if ratio > 0.1: return "High" 
        if ratio > 0.05: return "Medium"
        return "Low"
    
    def _get_risk_level(self, risk_score: float) -> str:
        if risk_score > 0.7: return "Critical"
        if risk_score > 0.5: return "High"
        if risk_score > 0.3: return "Medium"
        return "Low"
    
    def _get_column_issues(self, col_data: Dict) -> List[str]:
        issues = []
        if col_data.get('dlp_info_types'):
            issues.append(f"Sensitive data: {', '.join(col_data['dlp_info_types'][:3])}")
        if col_data.get('stats', {}).get('null_pct', 0) > 0.3:
            issues.append(f"High null percentage: {col_data['stats']['null_pct']*100:.1f}%")
        return issues
    
    def _get_column_recommendation(self, col_data: Dict) -> str:
        if col_data.get('dlp_info_types'):
            return "Apply data masking/encryption and review access controls"
        if col_data.get('stats', {}).get('null_pct', 0) > 0.3:
            return "Investigate data collection process and implement validation"
        return "No immediate action required"
    
    def _generate_detailed_findings(self, results: Dict) -> Dict:
        """Generate detailed technical findings"""
        return {
            "data_quality_metrics": self._extract_quality_metrics(results),
            "sensitive_data_breakdown": self._extract_sensitive_data_breakdown(results),
            "performance_metrics": {
                "profiling_time_seconds": results.get("execution_time_sec", 0),
                "rows_processed": results.get("rows_profiled", 0),
                "columns_analyzed": results.get("columns_profiled", 0)
            }
        }
    
    def _extract_quality_metrics(self, results: Dict) -> Dict:
        result_table = results.get("result_table", {})
        return {
            "average_null_percentage": self._calculate_average_metric(result_table, 'null_pct'),
            "average_uniqueness": self._calculate_average_metric(result_table, 'distinct_pct'),
            "columns_with_quality_issues": len([c for c in result_table.values() 
                                              if c.get('stats', {}).get('null_pct', 0) > 0.1])
        }
    
    def _extract_sensitive_data_breakdown(self, results: Dict) -> Dict:
        result_table = results.get("result_table", {})
        breakdown = {}
        for col_data in result_table.values():
            for info_type in col_data.get('dlp_info_types', []):
                clean_type = info_type.split(' (x')[0]
                breakdown[clean_type] = breakdown.get(clean_type, 0) + 1
        return breakdown
    
    def _calculate_average_metric(self, result_table: Dict, metric: str) -> float:
        values = [c.get('stats', {}).get(metric, 0) for c in result_table.values()]
        return sum(values) / len(values) if values else 0