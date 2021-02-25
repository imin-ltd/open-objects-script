const geolib = require('geolib');

// EventAttendanceMode defaults to offline
const getEventsEventAttendanceMode = (item) => item.eventAttendanceMode || 'https://schema.org/OfflineEventAttendanceMode';
const doesEventSupportOnline = (item) => ['https://schema.org/OnlineEventAttendanceMode', 'https://schema.org/MixedEventAttendanceMode'].includes(getEventsEventAttendanceMode(item));
const doesEventSupportOffline = (item) => ['https://schema.org/OfflineEventAttendanceMode', 'https://schema.org/MixedEventAttendanceMode'].includes(getEventsEventAttendanceMode(item));

/**
 * @param {{[k: string]: any}} mergedScheduledSession ScheduledSession which has had SessionSeries data merged in
 *   (therefore, it will have fields like `location`).
 * @param {import('./config').ConfigSegmentType[]} segments
 * @returns {string[]} Array of segment identifiers that this ScheduledSession belongs to.
 */
function generateSegmentIdentifiersForMergedScheduledSession(mergedScheduledSession, segments) {
  const physicalLocationGeo = (mergedScheduledSession.location && mergedScheduledSession.location.geo)
    || (mergedScheduledSession['beta:affiliatedLocation'] && mergedScheduledSession['beta:affiliatedLocation'].geo);
  const doesSupportOnline = doesEventSupportOnline(mergedScheduledSession);
  const doesSupportOffline = doesEventSupportOffline(mergedScheduledSession);
  if (!physicalLocationGeo && doesSupportOffline) {
    // Offline sessions that have no geo are invalid. So, we'll just ignore them
    return [];
  }
  return segments
    // First, filter only segments whose attendance mode filter matches this ScheduledSession's attendance mode.
    .filter((segment) => {
      switch (segment.attendanceModeFilter) {
        case 'all':
          return true;
        case 'physical-only':
          return doesSupportOffline;
        case 'virtual-only':
          return doesSupportOnline;
        default:
          throw new Error(`unrecognised attendanceModeFilter: ${segment.attendanceModeFilter}`);
      }
    })
    // Then, filter segments based on physical location (if there is one)
    .filter((segment) => {
      // If the session has no location (i.e. is fully online), we add it to all segments
      if (!physicalLocationGeo) { return true; }
      const segmentRadiusInMeters = segment.radius * 1000;
      const distanceBetweenGeos = geolib.getDistance(
        { latitude: physicalLocationGeo.latitude, longitude: physicalLocationGeo.longitude },
        { latitude: segment.latitude, longitude: segment.longitude },
      );
      return distanceBetweenGeos <= segmentRadiusInMeters;
    })
    .map((segment) => segment.identifier);
}

module.exports = {
  generateSegmentIdentifiersForMergedScheduledSession,
};
