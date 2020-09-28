/* eslint-disable no-mixed-operators */
const moment = require('moment-timezone');
const { getIsoWeekdays } = require('./getIsoWeekdays');

/**
 * @typedef {import('moment-timezone').Moment} Moment
 */

const SCHEDULED_SESSION = 'ScheduledSession';

// Preferable to using `null` because non-strict TS will handle them properly
const INFINITE_FUTURE = Symbol('A date that\'s infinitely far into the future. All other dates precede it');
const INFINITE_PAST = Symbol('A date that\'s infinitely far into the past. This data precedes all others');

const SCHEDULE_TIME_WINDOW_IN_WEEKS = 4;

/**
 * @typedef {{
 *   byDay?: string[],
 *   startTime: string,
 *   endTime: string,
 *   startDate: string,
 *   endDate: string,
 *   scheduleTimezone: string,
 *   duration?: string,
 *   idTemplate: string,
 * }} MinimalEventSchedule Note that the required fields map to the JOI schema
 */

/**
 * @param {string} dateString e.g. '2020-05-14'
 * @param {string} timeString e.g. '18:30'
 * @param {string} timezone IANA timezone e.g. 'Europe/London'
 */
function combineYYYYMMDDAndHHMMString(dateString, timeString, timezone) {
  return moment.tz(`${dateString} ${timeString}`, 'YYYY-MM-DD HH:mm', timezone);
}

/**
 * @param {MinimalEventSchedule} eventSchedule
 */
function getScheduleTimezone(eventSchedule) {
  return eventSchedule.scheduleTimezone || 'Europe/London';
}

/**
 * @param {MinimalEventSchedule} eventSchedule
 */
function* generateDatesWithinTimeWindow(eventSchedule) {
  const scheduleTimezone = getScheduleTimezone(eventSchedule);
  const nowMoment = moment().tz(scheduleTimezone);
  // ## Get end datetime
  const timeWindowEndDate = moment(nowMoment).add(SCHEDULE_TIME_WINDOW_IN_WEEKS, 'weeks');
  const scheduleEndDate = eventSchedule.endDate
    ? combineYYYYMMDDAndHHMMString(eventSchedule.endDate, eventSchedule.startTime, scheduleTimezone)
    : INFINITE_FUTURE;
  const latestMoment = (scheduleEndDate === INFINITE_FUTURE || timeWindowEndDate.isBefore(scheduleEndDate))
    ? timeWindowEndDate
    : scheduleEndDate;
  // ## Get earliest datetime
  //
  // Get the earliest datetime from which to start generating dates in the
  // future.
  //
  // The current date combined with the startTime (i.e. what if the schedule started today)
  const todayWithStartTimeMoment = combineYYYYMMDDAndHHMMString(nowMoment.format('YYYY-MM-DD'), eventSchedule.startTime, scheduleTimezone);
  // If that date ^ is in the past, add a day, so that it is the next time
  // after now that the schedule could take place when taking only startTime
  // into account (i.e. before looking at start/end dates & week days).
  const nextStartTimeAfterNowMoment = todayWithStartTimeMoment.isBefore(nowMoment)
    ? moment(todayWithStartTimeMoment).add(1, 'day')
    : todayWithStartTimeMoment;
  const scheduleStartDate = eventSchedule.startDate
    ? combineYYYYMMDDAndHHMMString(eventSchedule.startDate, eventSchedule.startTime, scheduleTimezone)
    : INFINITE_PAST;
  const earliestMoment = (scheduleStartDate === INFINITE_PAST || nextStartTimeAfterNowMoment.isAfter(scheduleStartDate))
    ? nextStartTimeAfterNowMoment
    : scheduleStartDate;
  // Optimisation: There's no point doing any more computation.
  // This will happen if the schedule start date is beyond the time window,
  // for example.
  if (earliestMoment.isAfter(latestMoment)) {
    return;
  }
  // ## Get weekday offsets
  //
  // These are used to generate dates according to each weekday on which this
  // schedule takes place.
  //
  const earliestIsoWeekday = earliestMoment.isoWeekday();
  // Calculate offsets between `earliestMoment` and each of the possible
  // weekdays on which this event can occur.
  const isoWeekdayOffsets = getIsoWeekdays(eventSchedule, scheduleTimezone)
    // They are sorted so that the resulting ScheduledSessions become ordered.
    // Additionally, this means that generation can stop as soon as the
    // `endDate` has been reached or surpassed without worrying if we've missed
    // any.
    .sort()
    .map((isoWeekday) => (
      // The number of days from the earliest date until this day (in this
      // week). e.g. if `earliestMoment` is on a Tuesday and this `isoWeekday`
      // is on a Thursday, the offset is 2. If `isoWeekday` is on a Monday,
      // the offset is -1.
      (isoWeekday - earliestIsoWeekday) % 7
    ));
  // ## Accumulate dates
  function* generateScheduledSessionStartDatesEndlessly() {
    // ### First week
    //
    // The first week is a special case as we may start somewhere in the middle
    // of this week. Some/all of the `byDay` days may already have passed,
    // so we will skip them for the first week.
    for (const isoWeekdayOffset of isoWeekdayOffsets) {
      // this list is sorted so branch prediction+
      if (isoWeekdayOffset < 0) { continue; }
      yield moment(earliestMoment).add(isoWeekdayOffset, 'days');
    }
    // ### Further weeks
    for (let weekOffset = 1; ; weekOffset += 1) {
      // It is important that this list have at least one item, otherwise
      // this will loop infinitely.
      for (const isoWeekdayOffset of isoWeekdayOffsets) {
        yield moment(earliestMoment).add((weekOffset * 7) + isoWeekdayOffset, 'days');
      }
    }
  }
  for (const scheduledSessionStartDate of generateScheduledSessionStartDatesEndlessly()) {
    if (scheduledSessionStartDate.isAfter(latestMoment)) {
      return;
    }
    const scheduledSessionEndDate = combineYYYYMMDDAndHHMMString(
      scheduledSessionStartDate.format('YYYY-MM-DD'), eventSchedule.endTime, scheduleTimezone);
    const scheduledSession = {
      type: SCHEDULED_SESSION,
      id: eventSchedule.idTemplate.replace('{startDate}', moment(scheduledSessionStartDate).utc().format()),
      startDate: moment(scheduledSessionStartDate).utc().format(),
      endDate: moment(scheduledSessionEndDate).utc().format(),
    };
    // Add duration if it exists
    if (eventSchedule.duration) {
      scheduledSession.duration = eventSchedule.duration;
    }
    yield scheduledSession;
  }
}

module.exports = {
  generateDatesWithinTimeWindow,
};
