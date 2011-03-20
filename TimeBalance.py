from itertools import izip, count
from operator import itemgetter
from datetime import timedelta
import numpy as np

# given an iterable of pairs return the key corresponding to the greatest value
def argmax(pairs):
    return max(pairs, key=itemgetter(1))[0]

# given an iterable of values return the index of the greatest value
def argmax_index(values):
    return argmax(izip(count(), values))

def argfind_index(item, values) :
    for index, aVal in enumerate(values) :
        if item is aVal :
            return index

    raise ValueError("Could not find item in list of values")


class TBScheduler(object) :
    def __init__(self, surveil_task, concurrent_max=1) :
        assert(concurrent_max >= 1)
        self.tasks = []
        self.T_B = []
        self.active_tasks = [None] * concurrent_max
        self._active_time = [timedelta() for index in range(concurrent_max)]

        self.surveil_task = surveil_task

        # The T_B value threshold for action
        self.actionTB = timedelta()

    def increment_timer(self, timeElapsed) :
        # Increment the T_B of all tasks by the selected task time
        # NOTE: can't do `tb += theTask.T` because += operator on
        #       a timedelta object isn't in-place. Go figure...
        for index in range(len(self.T_B)) :
            self.T_B[index] += timeElapsed

        for index, aTask in enumerate(self.active_tasks) :
            if aTask is not None :
                self._active_time[index] += timeElapsed

        # Remove tasks that are finished from the active slots.
        self.rm_deactive()

    def add_tasks(self, tasks) :
        # Initialize the T_B for each task
        self.T_B.extend([timedelta()] * len(tasks))
        self.tasks.extend(tasks)

    def rm_tasks(self, tasks) :
        # Slate these tasks for removal.
        # Note that you can't remove active tasks until they are done.
        # We will go ahead and remove any inactive tasks, and defer
        # the removal of active tasks until later.
        # Impementation Note: This actually isn't all that complicated,
        #                     due to the reference counting of python.
        #                     Just delete the task from the self.tasks
        #                     list and when the task is done in the
        #                     active list, it will finally be deleted.
        findargs = [argfind_index(aTask, self.tasks) for aTask in tasks]
        args = np.argsort(findargs)[::-1]
        for anItem in args :
            del self.tasks[findargs[anItem]]
            del self.T_B[findargs[anItem]]

    def is_available(self) :
        """
        Does the system have an available slot for a task execution?
        """
        return any([(activeTask is None) for
                     activeTask in self.active_tasks])

    def next_task(self) :
        if not self.is_available() :
            return None

        # -1 shall indicate to use the fallback task of surveillance.
        doTask = -1

        # Determine the next task to execute
        if len(self.tasks) > 0 :
            availTask = argmax_index(self.T_B)

            # FIXME: A bit rudimentary, but essentially,
            #       if the next task is not ready, then
            #       fall-back to the surveillance task.
            #       This should probably never happen if
            #       the update and time-fragment numbers
            #       are consistent, but this is just a
            #       robustness-check.
            if (not self.tasks[availTask].is_running and
                self.T_B[availTask] >= self.actionTB) :
                doTask = availTask
                # decrement the T_B of the task by the update time.
                self.T_B[availTask] -= self.tasks[availTask].U

        theTask = self.tasks[doTask] if doTask >= 0 else self.surveil_task

        # Another check to see if the task is already active.
        # Essentially, we are seeing if the surveillance task
        # is already active.
        # In which case, just return None.
        if theTask.is_running :
            return None
        else :
            self.add_active(theTask)
            return theTask

    def add_active(self, theTask) :
        for index, activeTask in enumerate(self.active_tasks) :
            if activeTask is None :
                theTask.is_running = True
                self.active_tasks[index] = theTask
                self._active_time[index] = timedelta()
                break

    def rm_deactive(self) :        
        for index, aTask in enumerate(self.active_tasks) :
            if aTask is not None :
                if self._active_time[index] >= aTask.T :
                    # The task is finished its fragment!
                    aTask.is_running = False
                    self._active_time[index] = timedelta()
                    self.active_tasks[index] = None





if __name__ == '__main__' :
    import ScanRadSim.task as task

    #import matplotlib.pyplot as plt

    # Just a quick test...
    surveillance = task.Task(timedelta(seconds=1),
                             timedelta(seconds=1),
                             range(10))
    tasks = [task.Task(update, time, radials) for update, time, radials
             in zip((timedelta(seconds=20), timedelta(seconds=35)),
                    (timedelta(seconds=10), timedelta(seconds=14)),
                    (range(30), range(14)))]
    sched = TBScheduler(surveillance)

    timer = timedelta(seconds=0)
    time_increm = timedelta(seconds=1)
    sched.add_tasks(tasks)
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
    while timer.seconds < 110 :
        theTask = sched.next_task()
        sched.increment_timer(time_increm)
        if theTask is not None :
            print "%.3d %.2d %.2d" % (timer.seconds, theTask.T.seconds, theTask.U.seconds)

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
