import { HistoricalDailyRegistry } from './HistoricalDailyRegistry.js';
import { ConsolidatedReports } from './ConsolidatedReports.js';

export const Reports = (mount, deps = {}, options = {}) => {
  const variant = String(options?.variant || 'daily').trim().toLowerCase();
  if (variant === 'company' || variant === 'consolidated') return ConsolidatedReports(mount, deps);
  return HistoricalDailyRegistry(mount, deps);
};
