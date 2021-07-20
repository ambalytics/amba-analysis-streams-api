import http from "../http-common";

class EventService {
  getAll() {
    return http.get("/event");
  }

  get(id) {
    return http.get(`/event/${id}`);
  }

  newest() {
    return http.get("/event/newest");
  }
}

export default new EventService();
