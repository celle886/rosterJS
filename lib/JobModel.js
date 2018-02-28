let knex = require('knex');

class JobModel {

  constructor (mysql) {

    this.db = knex(mysql);
  }

  // TODO
  findJobs () {}

  async findRunningJobs (limit, sort) {
    const runningJobs = await this.db
                                  .select('id', 'title', 'status', 'type', 'interval', 'nextRunAt', 'data')
                                  .where('status', 'running')
                                  .from('jobs');

    runningJobs.forEach(job => job.data = JSON.parse(job.data));

    return runningJobs;
  }

  async findPendingJobs (limit) {
    const pendingJobs = await this.db
                                  .select('id', 'title', 'status', 'type', 'interval', 'nextRunAt', 'data' )
                                  .where('status', 'pending')
                                  .limit(limit)
                                  .orderBy('nextRunAt', 'desc')
                                  .from('jobs');

    pendingJobs.forEach(job => job.data = JSON.parse(job.data));

    return pendingJobs;
  }

  async createNewJob (data) {
    data.data = JSON.stringify(data.data || {});

    const createdID = await this.db.insert(data).into('jobs')

    return createdID;
  }

  async updateJob (job) {

    job.data = JSON.stringify(job.data);

    const updated = await this.db('jobs')
            .where('id', job.id)
            .update(job)

    return updated;
  }

  async cleanUp () {
    await this.db('jobs').truncate()
  }

  async removeJobs (query) {
    const deletedJobs = this.db('jobs')
              .where(query)
              .del()

    return deletedJobs;
  }

  removeDoneJobs (query) {
    return this.removeJobs({ status: 'done' });
  }
} 

module.exports = JobModel;