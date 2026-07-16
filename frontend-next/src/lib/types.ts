/**
 * Server-mirror types uit api/main.py + supabase schema.
 * Handmatig onderhouden tot we OpenAPI schema genereren.
 */

export type SectorKey = 'cosmetische_behandelaars' | 'alternatieve_geneeskunde';

export const SECTOR_LABEL: Record<string, string> = {
  cosmetische_behandelaars: 'Cosmetisch',
  alternatieve_geneeskunde: 'Alt. geneeskunde',
  makelaars: 'Makelaars (archived)',
  bouwbedrijven: 'Bouw (archived)',
};

export type LeadStatus =
  | 'discovered'
  | 'enriched'
  | 'disqualified'
  | 'archived'
  | 'unsubscribed'
  | 'forgotten';

export type CrmStage =
  | 'ontdekt'
  | 'review_verstuurd'
  | 'gereageerd'
  | 'gesprek_gepland'
  | 'offerte_verstuurd'
  | 'gewonnen'
  | 'verloren'
  | 'later';

export interface Lead {
  id: string;
  workspace_id: string;
  company_name: string | null;
  domain: string | null;
  city: string | null;
  sector: string | null;
  phone: string | null;
  email: string | null;
  email_status: string | null;
  contact_first_name: string | null;
  google_rating: number | null;
  google_review_count: number | null;
  google_category: string | null;
  google_maps_url: string | null;
  score: number | null;
  website_score: number | null;
  icp_match: number | null;
  status: LeadStatus | string | null;
  crm_stage: CrmStage | string | null;
  enrichment_version: number | null;
  booking_system?: string | null;
  latest_review_date?: string | null;
  website_age_years?: number | null;
  meta_ads_active?: boolean | null;
  ad_focus?: string | null;
  treatment_focus?: string[] | null;
  personalized_opener?: string | null;
  personalization_hooks?: string[] | null;
  personalization_observations?: string[] | null;
  review_best_quote?: string | null;
  review_pain_points?: string[] | null;
  contact_why_chosen?: string | null;
  company_summary?: string | null;
  company_size_estimate?: string | null;
  has_instagram?: boolean | null;
  has_online_booking?: boolean | null;
  has_booking?: boolean | null;
  has_whatsapp?: boolean | null;
  cms_detected?: string | null;
  kvk_number?: string | null;
  kvk_sbi_code?: string | null;
  data_quality_score?: number | null;
  fit_score?: number | null;
  personalization_potential?: number | null;
  reachability_score?: number | null;
  archetype?: string | null;
  archetype_reason?: string | null;
  archetype_confidence?: number | null;
  is_test_lead?: boolean | null;
  last_call_at?: string | null;
  last_call_outcome?: CallOutcome | string | null;
  checkup_data?: CheckupData | null;
  created_at: string;
  updated_at?: string | null;
}

// --- Check-up follow-up (migratie 032) --------------------------------------
export type CallOutcome = 'won' | 'timing' | 'no_value' | 'stalled' | 'hard_no';
export type ReportStatus = 'pending' | 'generated' | 'approved' | 'sent' | 'skipped';
export type RetargetStatus = 'none' | 'scheduled' | 'sent' | 'replied' | 'exhausted';
export type MatchStatus = 'unmatched' | 'matched' | 'manually_matched';

export interface CheckupData {
  unanswered_inbound_per_week?: number;
  response_time_hours?: number;
  value_per_new_patient?: number;
  conversion_estimate?: number;
  no_shows_per_month?: number;
  hours_reception_per_week?: number;
  reviews_per_month?: number;
  treatments_per_month?: number;
  source?: string;
  captured_at?: string;
}

export interface ReportFinding {
  title?: string;
  fact?: string;
  cost?: string;
}

export interface CallRecord {
  id: string;
  workspace_id: string;
  lead_id: string | null;
  call_date: string;
  duration_minutes: number | null;
  transcript: string;
  participants: unknown[] | null;
  transcript_source: string;
  match_status: MatchStatus;
  outcome: CallOutcome | null;
  outcome_note: string | null;
  outcome_set_at: string | null;
  timing_target_date: string | null;
  report_findings: ReportFinding[] | null;
  report_html: string | null;
  report_status: ReportStatus;
  report_sent_at: string | null;
  retarget_due_at: string | null;
  retarget_status: RetargetStatus;
  retarget_attempt: number;
  retarget_last_sent_at: string | null;
  company_name?: string | null;   // join-veld in de Gesprekken-lijst
  created_at: string;
  updated_at: string;
}

// Koop-archetypes — bepaalt Mail 1 tone (zie enrichment/archetype_classifier.py)
export const ARCHETYPE_LABELS: Record<string, { label: string; sector: 'cosmetisch' | 'alternatief'; color: string }> = {
  // cosmetisch
  medisch_cosmetisch:        { label: 'Medisch-cosmetisch',     sector: 'cosmetisch',  color: 'bg-blue-100 text-blue-700' },
  esthetisch_medisch:        { label: 'Esthetisch-medisch',     sector: 'cosmetisch',  color: 'bg-purple-100 text-purple-700' },
  premium_beauty:            { label: 'Premium beauty',          sector: 'cosmetisch',  color: 'bg-pink-100 text-pink-700' },
  volume_beauty:             { label: 'Volume beauty',           sector: 'cosmetisch',  color: 'bg-amber-100 text-amber-700' },
  // alternatief
  lichaamswerk_pragmatisch:  { label: 'Lichaamswerk',            sector: 'alternatief', color: 'bg-emerald-100 text-emerald-700' },
  holistisch_spiritueel:     { label: 'Holistisch',              sector: 'alternatief', color: 'bg-teal-100 text-teal-700' },
  therapeutisch_mentaal:     { label: 'Therapeutisch',           sector: 'alternatief', color: 'bg-indigo-100 text-indigo-700' },
  welzijn_praktisch:         { label: 'Welzijn-praktisch',       sector: 'alternatief', color: 'bg-lime-100 text-lime-700' },
};

export interface ScrapingJob {
  id: string;
  workspace_id: string;
  sector: string | null;
  city: string | null;
  search_query: string | null;
  source: string | null;
  status: 'pending' | 'running' | 'completed' | 'failed' | string;
  total_found: number | null;
  total_new: number | null;
  error_message: string | null;
  created_at: string;
}

export interface LeadsResponse {
  leads: Lead[];
  total: number;
}

export interface JobsResponse {
  jobs: ScrapingJob[];
}

export interface PipelineStats {
  total_leads: number;
  verified_emails: number;
  email_coverage_pct: number;
  email_breakdown: Record<string, number>;
  enriched_leads: number;
  sent_to_warmr: number;
  total_replies: number;
  reply_rate_pct: number;
  catchall_emails: number;
  not_found_emails: number;
  pending_emails: number;
  website_opportunities: number;
  open_tasks_today: number;
  leads_in_pipeline: number;
  won_this_month: number;
}

export interface EnrichmentCost {
  today_eur: number;
  daily_budget_eur: number;
  budget_remaining_eur: number;
  budget_pct_used: number;
  budget_warning: boolean;
  window_days: number;
  window_eur: number;
  month_eur: number;
  monthly_budget_eur: number;
  monthly_base_eur?: number;
  monthly_override_eur?: number;
  override_info?: { extra_eur?: number; approved_at?: string; by?: string } | null;
  month_pct_used: number;
  month_remaining_eur: number;
  tier_50_hit: boolean;
  tier_100_hit: boolean;
  approved_over_50: boolean;
  enrichment_allowed: boolean;
  approval_required: boolean;
  block_reason: string | null;
  by_context: Record<string, number>;
  block_events_in_window: number;
}
