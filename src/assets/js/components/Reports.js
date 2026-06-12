import { DailyReports } from './DailyReports.js';
import { ConsolidatedReports } from './ConsolidatedReports.js';

export const Reports = (mount, deps = {}, options = {}) => {
  const variant = String(options?.variant || 'daily').trim().toLowerCase();
  if (variant === 'company' || variant === 'consolidated') return ConsolidatedReports(mount, deps);
  return DailyReports(mount, deps);
};
