"use strict";
let knex = require('knex');
let _ = require('lodash');

let request = require('request');
let moment = require('moment');

let JobModel = require('./JobModel');

class Jobs {

    constructor (mysql, cleanUp) {
        
        this.db = knex(mysql);

        this.JobModel = new JobModel(mysql);

        this.jobDefinitions = {};

        if (cleanUp) {
            this.JobModel.cleanUp()
        }
    }

    async createNewJob (title, options) {
        
        if (options.overwrite) {
            // only one instance of this job (by title) allowed to exist            
            await this.JobModel.removeJobs({ title: title });
        }

        this._createNewJob(title, options);
    }

    async _createNewJob (title, options) {
        
        if (!title) {

            return new Error('no title');
        }

        let newJob = {
            title: title,
            status: 'pending',
            data: JSON.stringify(options.data || {})
        }

        if (options.schedule) {
            newJob.nextRunAt = options.schedule;
        }

        if (options.type === 'interval') {
            newJob.type = 'interval';
            newJob.interval = options.interval;
        }

        const createdJobId = await this.JobModel.createNewJob(newJob);

        return createdJobId;
    }

    defineJob (title, callback) {

        this.jobDefinitions[title] = callback;

        return; 
    }

    start (config) {
        this.processInterval = setInterval(async () => {

            let runningJobs = await this.JobModel.findRunningJobs();
            let now = moment();

            console.log('runningJobs.length', runningJobs.length);

            if (runningJobs.length >= config.maxConcurreny) {
                return;
            }

            let pendingJobs = await this.JobModel.findPendingJobs((config.maxConcurreny - runningJobs.length), 'nextRunAt', -1);

            pendingJobs.forEach(job => {
                if (this.jobDefinitions[job.title])  {

                    if (moment().isAfter(job.nextRunAt)){

                        this.jobDefinitions[job.title](job, () => {
                            
                            if (job.type === 'single') {
                                job.status = 'done';    
                            }

                            if (job.type === 'interval') {
                                job.status = 'pending';
                                job.nextRunAt = moment(job.nextRunAt).add(job.interval, 'ms').toDate();
                            }
                            
                            this.JobModel.updateJob(job);
                        });
                        job.status = 'running';
                    }
                } else {
                    job.status = 'definition missing';
                }
                
                this.JobModel.updateJob(job);
            });

            await this.JobModel.removeDoneJobs();

            console.log('--------------- crossscore jobs process ------------------');

        }, config.processEvery)

        console.log('--------------- crossscore jobs is initialized ------------------');
    }

}

module.exports = Jobs;