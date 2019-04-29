
import pickle, os.path

from componentiser import Componentiser

from utils import newline_msg

import time, calendar


class SequenceCalculator:
    '''
        Class that allows for sequential processi
    '''


    def __init__(self, options):
        self.currency = options.currency
        self.heuristic = options.heuristic
        self.save_each = options.save_each
        self.log_filename =  options.log_filename

        try:
            self.epoch = options.epoch
        except: pass

        self.output_dir = "UH-%s-%s" % (self.currency, self.heuristic)

        self.min_blk = options.min_blk
        self.max_blk = options.max_blk

        self.period = options.period
        try:
            self.load_period = options.load_period
        except:
            self.load_period = options.period

        cfg_filename = "%s/config-%d_%d.pickle" % (self.output_dir, options.load_period[0], options.load_period[1])

        newline_msg("LOAD", "configuration %s" % cfg_filename)
        self.file_log = open(options.log_filename, "wa")

        self.cfg = pickle.load(open(cfg_filename))

        self.n_users = self.cfg["n_users"]

        try:
            self.minimised_n_users = self.cfg["minimised_n_users"]
        except: pass

        self.block_id = self.min_blk





    def load_blk(self, id):
        """Returns a BlockParser object"""
        outdir_name = "%s/%.3d"%(self.output_dir, id/1000)
        return pickle.load(open("%s/%d.pickle"%(outdir_name, id), "r"))

    def get_out_filename(self, calculation, extension):
        return "CALC-%s-%s-%s_%s.%s"%(calculation, self.heuristic ,self.period[0], self.period[1], extension )

    def process_all(self):
        for id in range(self.min_blk, self.max_blk):
            blk = self.load_blk(id)  # :::~ This loads the first block
            self.process(blk)

            if self.update_condition(block_id=blk.block_id):
                self.update_intermediate(block_id=blk.block_id, proc="process_all")

        self.update_intermediate(block_id=self.max_blk)


    def update_intermediate(self, **kwargs):
        additional = ""
        if kwargs.has_key("proc"):
            additional = kwargs["proc"]
            # :::~ function called every save_each
        newline_msg("MSG", "%s @blk:%s . #user = %8d .... #apply: %d " % (additional, kwargs["block_id"], self.n_users, self.component_id.counter))
        if self.file_log:
            newline_msg("MSG", "%s @blk:%s . #user = %8d .... #apply: %d " % (additional, kwargs["block_id"], self.n_users, self.component_id.counter),
                        stream=self.file_log)
            self.file_log.flush()

    def get_time(self, blk):
        tm_struct = time.strptime(blk.time, "%Y-%m-%d %H:%M:%S")
        if self.epoch == "day":
            tm_float = calendar.timegm((tm_struct.tm_year, tm_struct.tm_mon, tm_struct.tm_mday, 0, 0, 0, 0, 0, 0))
        elif self.epoch == "month":
            tm_float = calendar.timegm((tm_struct.tm_year, tm_struct.tm_mon, 1, 0, 0, 0, 0, 0, 0))
        elif self.epoch == "hour":
            tm_float = calendar.timegm(
                (tm_struct.tm_year, tm_struct.tm_mon, tm_struct.tm_mday, tm_struct.tm_hour, 0, 0, 0, 0, 0))
        else:
            tm_float = time.mktime(tm_struct)

        return tm_float

    def update_condition(self, **kwargs):
        if self.epoch == 'block':
            return kwargs['block_id'] % self.save_each == 0
        elif self.epoch == 'day':
            return self.current_time > self.last_time
        elif self.epoch == 'month':
            return self.current_time > self.last_time
