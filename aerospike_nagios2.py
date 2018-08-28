#!/usr/bin/env python

import sys
import popen2
import argparse


class AsinfoException(Exception):
    def __init__(self, error_text=""):
        self.error_text = error_text
        super(AsinfoException, self).__init__()


class AerospikeChecker(object):
    OK = 0
    WARNING = 1
    CRITICAL = 2

    def __init__(self, args):
        self.cluster_size = args.cluster_size
        self.free_pct_memory = args.free_pct_memory
        self.free_pct_disk = args.free_pct_disk
        self.ns_available_pct = args.ns_available_pct
        self.ns_disk_free_pct = args.ns_disk_free_pct
        self.ns_mem_free_pct = args.ns_mem_free_pct
        self.evicted_objects = args.evicted_objects
        self.stats = self.load_stats()

    def system(self, cmd):
        return popen2.popen2(cmd)[0].read().strip()

    def load_stats(self):
        ret = self.load_asinfo("statistics")
        ret["namespaces"] = {}
        for ns in self.load_asinfo("namespaces"):
            ret["namespaces"][ns] = self.load_asinfo("namespace/%s" % ns)
        return ret

    def load_asinfo(self, cmd):
        res = self.system("asinfo -v '%s'" % cmd)
        if "=" in res:
            try:
                ret = {}
                for pair in res.split(";"):
                    k, v = pair.split("=")
                    if v.isdigit():
                        v = int(v)
                    ret[k] = v
            except Exception:
                raise AsinfoException(error_text=res)
        elif ";" in res:
            ret = res.split(";")
        else:
            raise AsinfoException(error_text=res)
        return ret

    def run_checks(self):
        checks = {
                "Cluster size": self.check_cluster_size,
                "Writes": self.check_writes,
                "Free memory": self.check_free_memory,
                "NS available disk": self.check_ns_available,
                "NS free disk": self.check_ns_disk_free,
                "NS free memory": self.check_ns_mem_free,
                "Evicted objects": self.check_evicted_objects,
                }
        ok_text = []
        fail_text = []
        ret_code = self.OK
        levels = ["OK", "WARNING", "CRITICAL"]
        for check_name, method in checks.items():
            result, text = method()
            if result > ret_code:
                ret_code = result
            if result == self.OK:
                ok_text.append("%s %s - %s" % (check_name, levels[result], text))
            else:
                fail_text.append("%s %s - %s" % (check_name, levels[result], text))

        text = ". ".join((
                    ". ".join(fail_text),
                    ". ".join(ok_text),
                )).strip(". ")

        return ret_code, text

    def check_evicted_objects(self):
        warning, critical = [int(i) for i in self.evicted_objects.split(":")]
        result = self.OK
        res_text = []
        for ns, stats in self.stats["namespaces"].items():
            val = stats.get("evicted_objects")
            res_text.append("%s=%i" % (ns, val))
            if val >= warning:
                result = self.WARNING
            if val >= critical:
                result = self.CRITICAL
        return result, ", ".join(res_text)

    def check_writes(self):
        result = self.OK
        text = []
        for ns, stats in self.stats["namespaces"].items():
            if stats.get("stop_writes") == "true":
                result = self.CRITICAL
                text.append("%s=FAIL" % ns)
            else:
                text.append("%s=OK" % ns)
        return result, ", ".join(text)

    def check_ns_available(self):
        warning, critical = [int(i) for i in self.ns_available_pct.split(":")]
        result = self.OK
        res_text = []
        for ns, stats in self.stats["namespaces"].items():
            val = stats.get("device_available_pct", 100)
            res_text.append("%s=%i%%" % (ns, val))
            if val <= warning:
                result = self.WARNING
            if val <= critical:
                result = self.CRITICAL
        return result, ", ".join(res_text)

    def check_ns_disk_free(self):
        warning, critical = [int(i) for i in self.ns_disk_free_pct.split(":")]
        result = self.OK
        res_text = []
        for ns, stats in self.stats["namespaces"].items():
            val = stats.get("device_free_pct", 100)
            res_text.append("%s=%i%%" % (ns, val))
            if val <= warning:
                result = self.WARNING
            if val <= critical:
                result = self.CRITICAL
        return result, ", ".join(res_text)

    def check_ns_mem_free(self):
        warning, critical = [int(i) for i in self.ns_mem_free_pct.split(":")]
        result = self.OK
        res_text = []
        for ns, stats in self.stats["namespaces"].items():
            val = stats.get("memory_free_pct", 100)
            res_text.append("%s=%i%%" % (ns, val))
            if val <= warning:
                result = self.WARNING
            if val <= critical:
                result = self.CRITICAL
        return result, ", ".join(res_text)

    def check_free_memory(self):
        warning, critical = [int(i) for i in self.free_pct_memory.split(":")]
        val = self.stats.get("system_free_mem_pct")
        result = self.OK
        if val <= warning:
            result = self.WARNING
        if val <= critical:
            result = self.CRITICAL
        return result, "%i%%" % val

    def check_cluster_size(self):
        result = self.OK
        if self.stats["cluster_size"] < self.cluster_size:
            result = self.CRITICAL
        return result, str(self.stats["cluster_size"])


def main():
    parser = argparse.ArgumentParser(
            "Aerospike health checker,\nsee key metrics desription at http://www.aerospike.com/docs/operations/monitor/key_metrics\n",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--cluster-size", help="cluster size", type=int, default=4)
    parser.add_argument("--free-pct-memory", help="free memory %% - warning:critical", default="20:10")
    parser.add_argument("--free-pct-disk", help="free disk %% - warning:critical", default="20:10")
    parser.add_argument("--ns-available-pct", help="Disk available %% - warning:critical", default="20:15")
    parser.add_argument("--ns-disk-free-pct", help="Disk free %% - warning:critical", default="25:15")
    parser.add_argument("--ns-mem-free-pct", help="Memory free %% - warning:critical", default="25:15")
    parser.add_argument("--evicted-objects", help="evicted objects - warning:critical", default="10000000:50000000")

    args = parser.parse_args()
    try:
        checker = AerospikeChecker(args)
        exit_code, text = checker.run_checks()
    except AsinfoException as e:
        exit_code, text = 2, "CRITICAL: asinfo error, text returned - \"%s\"" % e.error_text.replace("\n", " | ")
    except Exception as e:
        exit_code, text = 2, str(e)
    print(text)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
