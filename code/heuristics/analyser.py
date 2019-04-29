
import pickle, os.path

from componentiser import Componentiser

from utils import newline_msg


class SequenceAnalyser:
    '''
        Class that allows for sequential processi
    '''


    def __init__(self, options):
        self.currency = options.currency
        self.save_each = options.save_each
        self.log_filename =  options.log_filename

        try:
            self.previous = options.previous
            self.previous_dir = options.previous_dir

        except: pass
        try:
            self.base_heuristic = options.base_heuristic
            self.base_dir = options.base_dir
        except: pass

        self.heuristic = options.heuristic

        self.output_dir = options.output_dir

        self.min_blk = options.min_blk
        self.max_blk = options.max_blk

        self.component_id = Componentiser()

        self.n_users = 0

        self.file_log = open(options.log_filename, "wa")


    def load_blk(self, id):
        """Returns a BlockParser object"""
        outdir_name = "%s/%.3d"%(self.previous_dir, id/1000)
        return pickle.load(open("%s/%d.pickle"%(outdir_name, id), "r"))


    def process(self,blk):
        pass

    def process_all(self, id_min, id_max):
        self.prefix_kw = {"f": "process_all"}
        self.postfix_kw = {}

        for id in range(id_min, id_max):
            blk = self.load_blk(id)  # :::~ This loads the first block
            self.process(blk)

            if self.update_condition(block_id=blk.block_id):
                self.update_intermediate(block_id=blk.block_id, proc="process_all")

        self.update_intermediate(block_id=id_max)
        self.prefix_kw = {}
        self.postfix_kw = {}


    def add_correspondence(self, ls, oth = None):
        self.component_id.add_correspondence(ls, oth)

    def update_n_users(self, ls):
        self.component_id.update_n_users(ls)
        self.n_users = self.component_id.n_users

    def compute_and_save_correspondences(self):
        self.component_id.compute_correspondences()
        self.component_id.save(self.output_dir,self.min_blk,self.max_blk)

    def update_condition(self, **kwargs):
        return kwargs['block_id'] % self.save_each == 0

    def update_intermediate(self, **kwargs):
        # :::~ function called every save_each
        prefix = ""
        if len(self.prefix_kw) > 0:
            prefix = " ".join([ "%s=%s"%(k,v) for k,v in sorted( self.prefix_kw.iteritems() ) ])

        postfix = ""
        if len(self.postfix_kw) > 0:
            postfix = " ".join([ "%s=%s"%(k,v) for k,v in sorted( self.postfix_kw.iteritems() ) ])


        newline_msg("MSG", "%s @blk:%s .... %s " % (prefix, kwargs["block_id"], postfix))
        if self.file_log:
            newline_msg("MSG", "%s @blk:%s .... %s " % (prefix, kwargs["block_id"], postfix),
                        stream=self.file_log)
            self.file_log.flush()

    def save_config(self, **kw):
        cfg_filename = "%s/config-%d_%d.pickle"%(self.output_dir,self.min_blk,self.max_blk)


        cfg = {}
        cfg["n_users"] = self.n_users
        cfg["min_blk"] = self.min_blk
        cfg["max_blk"] = self.max_blk
        cfg["heuristic"] = self.heuristic
        try:
            cfg["minimised_n_users"] = self.component_id.minimised_n_users
        except:
            pass
        for k in kw:
            cfg[k] = kw[k]

        pickle.dump(cfg,open(cfg_filename,"w"))

        newline_msg("OUT", "cfg: '%s'"%cfg_filename)
        for k, v in sorted(cfg.iteritems()):
            newline_msg("--- ", "  %s: %s" % (k,v), indent = 2)
#        pickle.dump(cfg, open("%s/config.pickle" % (self.output_directory), "w"))





class SequenceAmender(SequenceAnalyser):
    def __init__(self, options, load = True):
        SequenceAnalyser.__init__(self,options)

        if not load:
            return

        newline_msg("LOAD", "componentiser ")
        self.component_id.load(self.output_dir, self.min_blk, self.max_blk)


    def save_blk(self, blk):
        id_str = str(blk.block_id)
        outdir_name = "%s/%.3d" % (self.output_dir, int(id_str) / 1000)

        if not os.path.exists(outdir_name):
            os.makedirs(outdir_name)

        pickle.dump(blk, open("%s/%s.pickle" % (outdir_name, id_str), "w"))


#    def amend_blk(self,blk):
#        pass

    def amend_all(self, id_min, id_max):
        self.prefix_kw = {"f": "saving"}
        self.postfix_kw = {}

        for id in range(id_min, id_max):
            blk = self.load_blk(id) # :::~ This loads the first block

            blk = self.amend(blk)

#            print blk.short_transactions

            self.save_blk(blk)

            if self.update_condition(block_id = blk.block_id):
                self.update_intermediate(block_id = blk.block_id)
        self.prefix_kw = {}
        self.postfix_kw = {}

      #  cfg = {}
      #  cfg["no_users"] = self.component_id.minimised_no_users
      #  cfg["previous_no_users"] = self.n_users
      #  cfg["min_blk"] = self.min_blk
      #  cfg["max_blk"] = self.max_blk
      #  cfg["heuristics"] = self.heuristic

      #  pickle.dump(cfg ,open( "%s/config-%d_%d.pickle" %(self.output_dir, self.min_blk, self.max_blk) ,"w"))
      #  pickle.dump(cfg, open("%s/config.pickle" % (self.output_dir), "w"))

