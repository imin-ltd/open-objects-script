const moment = require('moment-timezone');

/**
 * @param {*} eventSchedule
 * @param {string} scheduleTimezone IANA timezone e.g. 'Europe/London'
 * @returns {number[]}
 */
function getIsoWeekdays(eventSchedule, scheduleTimezone) {
  if (!Array.isArray(eventSchedule.byDay)) {
    // If there is no byDay and no startDate, there is no information with
    // which to generate dates
    if (!eventSchedule.startDate) {
      return [];
    }
    // if byDay isn't specified, assume that the startDate's week day is the only week day that is recurring
    const isoWeekday = moment.tz(eventSchedule.startDate, 'YYYY-MM-DD', true, scheduleTimezone).isoWeekday();
    return [isoWeekday];
  }
  const isoWeekdays = [];
  for (const byDayItem of eventSchedule.byDay) {
    if (byDayItem.includes('Monday')) {
      isoWeekdays.push(1);
    } else if (byDayItem.includes('Tuesday')) {
      isoWeekdays.push(2);
    } else if (byDayItem.includes('Wednesday')) {
      isoWeekdays.push(3);
    } else if (byDayItem.includes('Thursday')) {
      isoWeekdays.push(4);
    } else if (byDayItem.includes('Friday')) {
      isoWeekdays.push(5);
    } else if (byDayItem.includes('Saturday')) {
      isoWeekdays.push(6);
    } else if (byDayItem.includes('Sunday')) {
      isoWeekdays.push(7);
    }
  }
  return isoWeekdays;
}

module.exports = {
  getIsoWeekdays,
};
