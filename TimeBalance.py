from itertools import izip, count
from operator import itemgetter
from datetime import timedelta
import numpy as np

from ScanRadSim.ScanSim import _to_seconds
from ScanRadSim.TaskScheduler import TaskScheduler

# given an iterable of pairs return the key corresponding to the greatest value
def argmax(pairs):
    return max(pairs, key=itemgetter(1))[0]

# given an iterable of values return the index of the greatest value
def argmax_index(values):
    return argmax(izip(count(), values))


class TBScheduler(TaskScheduler) :
    def __init__(self, surveil_job, concurrent_max=1) :
        TaskScheduler.__init__(self, concurrent_max)

        self.T_B = []
        self.surveil_job = surveil_job
        # The T_B value threshold for action
        self.actionTB = timedelta()



    def occupancy(self) :
        return sum([_to_seconds(aJob.T)/_to_seconds(aJob.U) for
                    aJob in (self.jobs + [self.surveil_job]) if
                    aJob.T is not None])

    def acquisition(self) :
        return max([_to_seconds(aJob.U) for aJob in
                     (self.jobs + [self.surveil_job])])

    def increment_timer(self, timeElapsed) :
        # Increment the T_B of all jobs by the elapsed time
        # NOTE: can't do `tb += timeElapsed` because += operator on
        #       a timedelta object isn't in-place. Go figure...
        for index in range(len(self.T_B)) :
            self.T_B[index] += timeElapsed

        TaskScheduler.increment_timer(self, timeElapsed)

    def add_jobs(self, jobs) :
        TaskScheduler.add_jobs(self, jobs)
        # Initialize the T_B for each job
        self.T_B.extend([timedelta()] * len(jobs))

    def rm_jobs(self, jobs) :
        findargs, args = TaskScheduler.rm_jobs(self, jobs)
        for anItem in args :
            del self.T_B[findargs[anItem]]

        return findargs, args

    def next_jobs(self) :
        next_jobs = []
        while self.is_available() :
            # -1 shall indicate to use the fallback job of surveillance.
            doJob = -1

            # Determine the next job to execute
            if len(self.jobs) > 0 :
                availJob = argmax_index(self.T_B)

                if self.T_B[availJob] >= self.actionTB :
                    doJob = availJob
                    # decrement the T_B of the job by the fractional update time.
                    self.T_B[availJob] -= (self.jobs[availJob].U / len(self.jobs[availJob]._origradials))

            theJob = self.jobs[doJob] if doJob >= 0 else self.surveil_job

            self.add_active(theJob)
            next_jobs.append(theJob)

        return next_jobs


if __name__ == '__main__' :
    import ScanRadSim.task as task

    #import matplotlib.pyplot as plt

    # Just a quick test...
    surveillance = task.StaticJob(timedelta(seconds=60), (range(1),),
                            timedelta(seconds=1))

    jobs = [task.StaticJob(update, radials, time) for update, time, radials
             in zip((timedelta(seconds=20), timedelta(seconds=35)),
                    (timedelta(seconds=10), timedelta(seconds=14)),
                    ((range(1),), (range(1),)))]
    sched = TBScheduler(surveillance)

    timer = timedelta(seconds=0)
    time_increm = timedelta(seconds=1)
    sched.add_jobs(jobs)
    """
    tb1 = []
    tb2 = []
    times1 = []
    times2 = []
    tb1.append(sched.T_B[0])
    tb2.append(sched.T_B[1])
    times1.append(timer)
    times2.append(timer)
    """
    print "Time Frag Updt"
    while timer.seconds < 110 :
        theJobs = sched.next_jobs()
        sched.increment_timer(time_increm)
        if len(theJobs) > 0 :
            print "%.3d  %2d   %2d" % (timer.seconds, theJobs[0].T.seconds, theJobs[0].U.seconds)

        """
        if theTask.name == 'foo' :
            times1.extend([timer, timer])
            tb1.extend([tb1[-1], sched.T_B[0]])

            times2.append(timer + theTask.T)
            tb2.append(sched.T_B[1])
        elif theTask.name == 'bar' :
            times2.extend([timer, timer])
            tb2.extend([tb2[-1], sched.T_B[1]])

            times1.append(timer + theTask.T)
            tb1.append(sched.T_B[0])
        """
        timer += time_increm



    """
    plt.plot(times1, tb1, 'r', label='task 1')
    plt.plot(times2, tb2, 'g', label='task 2') 
    plt.legend()
    plt.show()
    """
