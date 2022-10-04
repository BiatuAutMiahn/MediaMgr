import code
import sys
import os, fnmatch
import re
import time
from tmdbv3api import *
from tmdbv3api import exceptions
import tmdbv3api
import json
from subprocess import Popen,PIPE,STDOUT
from pathlib import Path
import selectors
from queue import Queue, Empty
from concurrent.futures import ThreadPoolExecutor
from mutagen.mp4 import MP4, MP4Cover
import http.client
import traceback
import pickle

tmdb = TMDb()
tmdb.api_key = ''
tmdb.language = 'en'
tmdb_mov = Movie()
tmdb_tv = TV()
tmdb_sea = Season()

queue=[[],[],[],[],[],[]]
ignore=[]
tvmc={}
mmc={}
tmdbdc={}
tmdbsc={}
err_log={}
movie_pat=[
    re.compile("^(.*)[\. ][\(\[]?(\d{4})[\)\]]?(:?[\. ].*|)$"),
    re.compile("(.*) (?:extended and unrated |extended unrated |directors cut |unrated |extended |dc )[\(\[]?(\d{4})[\)\]]? .*",re.IGNORECASE),
    re.compile("(.*) [\(\[]?(\d{4})[\)\]]? .*"),
    re.compile("^(.*) \(?(\d{4})\)$")
]
tv_pat=[
    re.compile("(.*)[\. ]S(\d{1,2})[\. ]?E(\d{1,3}).*",re.IGNORECASE),
    re.compile("^(.*)[\. ][\(\[]?(\d{4})[\)\]]?(:?[\. ].*|)$")
]
ffProgPat=re.compile("^frame=[ ]*(\d*) fps=[ ]*([\d.]*) q=(-?[\d.]*) L?size=[ ]*(\d*..|N/A) time=([\d:.]*) bitrate=[ ]*([\da-z./]*|N/A) speed=(.*)x$")
# encoutactPat=re.compile("")

def restart_line():
    sys.stdout.write('\r')
    sys.stdout.write('\r')
    sys.stdout.flush()

def enqueue_output(file, queue):
    for line in iter(file.readline, ''):
        queue.put(line)
    file.close()

def removeEmptyFolders(path, removeRoot=True):
  if not os.path.isdir(path):
    return
  files = os.listdir(path)
  if len(files):
    for f in files:
      fullpath = os.path.join(path, f)
      if os.path.isdir(fullpath):
        removeEmptyFolders(fullpath)
  files = os.listdir(path)
  if len(files) == 0 and removeRoot:
    print("Removing empty folder:",path)
    os.rmdir(path)

def read_popen_pipes(p):

    with ThreadPoolExecutor(2) as pool:
        q_stdout, q_stderr = Queue(), Queue()

        pool.submit(enqueue_output, p.stdout, q_stdout)
        pool.submit(enqueue_output, p.stderr, q_stderr)

        while True:

            if p.poll() is not None and q_stdout.empty() and q_stderr.empty():
                break

            out_line = err_line = ''

            try:
                out_line = q_stdout.get_nowait()
            except Empty:
                pass
            try:
                err_line = q_stderr.get_nowait()
            except Empty:
                pass

            yield (out_line, err_line)

def lsdexp(rd, p):
    for r, d, fl in os.walk(rd):
        for bn in fl:
            if re.match(p,bn): # fnmatch.fnmatch(basename, pattern):
                fn, fe=os.path.splitext(bn)
                yield [r+os.path.sep+bn,fn,fe]

def savQueState():
    with open('queue.dat','wb') as q:
        pickle.dump(queue,q)
        q.close()

if os.path.exists('queue.dat'):
    with open('queue.dat','rb') as q:
        queue=pickle.load(q)
        q.close()
print(queue)

for ca in ['tvmc','mmc','tmdbdc','tmdbsc']:
    if os.path.exists(ca):
        with open(ca,'rb') as q:
            queue=pickle.load(q)
            q.close()

while True:
    try:
        # Discovery
        for f in lsdexp('in'+os.path.sep, '.*(:?\.mkv|\.mp4|\.m4v|\.avi|\.mpg|\.ogm)$'):
            ign=False
            for stg in [0,1,2,3,4,5]:
                for i in queue[stg]:
                    if f[0]==i[0]:
                        ign=True
            for i in ignore:
                if f[0]==i[0]:
                    ign=True
            if not ign:
                print("    Queued: %s"%f[1]+f[2])
                queue[0].append(f)
            else:
                # print("    Ignored: %s"%f[1]+f[2])
                pass
        # exit()
        # Stage 0, Classification
        for f in queue[0].copy():
            if 'sample' in f[1].lower():
                ignore.append(f)
                queue[0].remove(f)
                # print("Ignored: %s"%f[1])
            elif re.match(tv_pat[0],f[1]):
                queue[0].remove(f)
                t=re.sub(tv_pat[0],"\\1",f[1].replace('.',' '))
                s=re.sub(tv_pat[0],"\\2",f[1].replace('.',' '))
                e=re.sub(tv_pat[0],"\\3",f[1].replace('.',' '))
                f.append(['tv',t,s,e])
                queue[1].append(f)
                print("isTVSeries: %s [%s S%sE%s]"%(f[0],t,s,e))
            elif re.match(movie_pat[0],f[1]):
                queue[0].remove(f)
                t=re.sub(movie_pat[0],"\\1",f[1].replace('.',' '))
                y=re.sub(movie_pat[0],"\\2",f[1].replace('.',' '))
                f.append(['movie',t,y])
                queue[1].append(f)
                print("isMovie: %s [%s (%s)]"%(f[0],t,y))
            else:
                ignore.append(f)
                queue[0].remove(f)
                print("UnhandledMedia: %s"%f[0])
        # Stage 1, GetMeta
        for f in queue[1].copy():
            savQueState()
            print('GetMeta:    %s'%(f[0]))
            m=f[3]
            if m[0]=='movie':
                s=None
                try:
                    s=tmdb_mov.search(m[1])
                    time.sleep(0.25)
                except Exception as e:
                    if tmdb_mov._remaining < 1:
                        awai=abs(tmdb_mov._reset-int(time.time()))
                        print("RateLimit: %s,Sleep(%s)"%(f[1],awai))
                        time.sleep(awai)
                    else:
                        print("MetaFail: %s,%s"%(f[1],e))
                    continue
                mo=None
                for i in s:
                    if not hasattr(i,'release_date'):
                        continue
                    if m[2]==i.release_date[0:4]:
                        mo=i
                        break
                if mo:
                    md=tmdb_mov.details(mo.id)#
                    time.sleep(0.25)
                    mod=[]
                    mod.append(md.title)
                    mod.append(md.release_date.replace('-','.'))
                    # if md.poster_path==None:
                    #     mod.append(md.poster_path)
                    # else:
                    mod.append(md.poster_path)
                    mod.append([d['name'] for d in md.genres])
                    mod.append([d['name'] for d in md.production_companies])
                    mod.append([d['name'] for d in md.casts['crew'] if d['job'].lower()=="director"])
                    mod.append([d['name'] for d in md.casts['crew'] if d['job'].lower()=="producer"])
                    mod.append([d['name'] for d in md.casts['cast']])
                    mod.append(md.overview)
                    f.append(mod)
                    queue[1].remove(f)
                    queue[2].append(f)
                    nfn=md.title+" ("+md.release_date[0:4]+')'
                    nfn=nfn.replace('?','')
                    nfn=nfn.replace('*','!')
                    nfn=nfn.replace('/',' ')
                    nfn=nfn.replace('\\',' ')
                    nfn=nfn.replace(':','-')
                    print('IdentMovie: %s'%(nfn))
                    f[1]=nfn
                else:
                    ignore.append(f)
                    queue[1].remove(f)
                    print(" IdentFail: %s"%f[1]+f[2])
                    continue
            elif m[0]=='tv':
                s=None
                # print(m)
                try:
                    if re.match(movie_pat[0],f[1]):
                        # print('MovPat')
                        t=re.sub(movie_pat[0],"\\1",f[1].replace('.',' '))
                        y=re.sub(movie_pat[0],"\\2",f[1].replace('.',' '))
                        if not t in tmdbsc:
                            s=tmdb_tv.search(t)
                            time.sleep(0.25)
                            tmdbsc[t]=s
                        else:
                            s=tmdbsc[t]
                        for so in s:
                            tvo=None
                            if not so.id in tmdbdc:
                                tvo=tmdb_tv.details(so.id)
                                time.sleep(0.25)
                                tmdbdc[so.id]=tvo
                            else:
                                tvo=tmdbdc[so.id]
                            if tvo.first_air_date!=None:
                                if tvo.first_air_date[0:4]==y:
                                    s=[so]
                    else:
                        # print('TvPat')
                        if not m[1] in tmdbsc:
                            s=tmdb_tv.search(m[1])
                            time.sleep(0.25)
                            tmdbsc[m[1]]=s
                        else:
                            s=tmdbsc[m[1]]
                except Exception as e:
                    if tmdb_tv._remaining < 1:
                        awai=abs(tmdb_tv._reset-int(time.time()))
                        print("RateLimit: %s,Sleep(%s)"%(f[1],awai))
                        time.sleep(awai)
                    else:
                        ignore.append(f)
                        queue[1].remove(f)
                        print("MetaFail: %s,%s"%(f[1],e))
                        continue
                if len(s)==0:
                    ignore.append(f)
                    queue[1].remove(f)
                    print("MetaFail: %s,%s"%(f[1],e))
                    continue
                tvs=None
                #
                # try:
                #     print(s[0].id)
                # except:
                #     print('Error')
                #     print(s)
                #     print(m)
                #     exit()
                if s[0].id not in tvmc:
                    md=None
                    mname=s[0].name
                    if not s[0].id in tmdbdc:
                        md=tmdb_tv.details(s[0].id)
                        time.sleep(0.25)
                        tmdbdc[s[0].id]=md
                    else:
                        md=tmdbdc[s[0].id]
                    tvs=[s[0].name,s[0].id,[d['name'] for d in md.genres],[d['name'] for d in md.created_by],[d['name'] for d in md.networks],[d['name'] for d in md.credits['cast']],{}]
                    tvsl=len(tvs)-1
                    for so in md.seasons:
                        if so['season_number']==0:
                            continue
                        tvs[tvsl][so['season_number']]=[None,so['episode_count'],{}]
                        if so['poster_path']==None:
                            tvs[tvsl][so['season_number']][0]=md.poster_path
                        else:
                            tvs[tvsl][so['season_number']][0]=so['poster_path']
                        tvssl=len(tvs[tvsl][so['season_number']])-1
                        sd=None
                        if not str(tvs[1])+'.'+str(so['season_number']) in tmdbdc:
                            sd=tmdb_sea.details(tvs[1],so['season_number'])
                            time.sleep(0.25)
                            tmdbdc[str(tvs[1])+'.'+str(so['season_number'])]=sd
                        else:
                            sd=tmdbdc[str(tvs[1])+'.'+str(so['season_number'])]
                        for e in sd.episodes:
                            if e['air_date']==None:
                                e['air_date']=""
                            epis=[e['name'],e['air_date'].replace('-','.'),[d['name'] for d in e['guest_stars']],e['overview'],s[0].id]
                            tvs[tvsl][so['season_number']][tvssl][e['episode_number']]=epis
                    tvmc[s[0].id]=tvs
                else:
                    tvs=tvmc[s[0].id]
                # print(m)
                # print(tvs)
                # exit()
                if int(m[2]) not in tvs[len(tvs)-1]:
                    ignore.append(f)
                    queue[1].remove(f)
                    print("UnhandledMeta: %s,Season.OutOfBounds"%f[0])
                    continue
                sd=tvs[len(tvs)-1][int(m[2])]
                if not int(m[3]) in sd[len(sd)-1]:
                    ignore.append(f)
                    queue[1].remove(f)
                    print("UnhandledMeta: %s,Episode.OutOfBounds"%f[0])
                    continue
                f[3].append(sd[len(sd)-1][int(m[3])])
                nfnb=f[3][len(f[3])-1][0]
                nfnb=nfnb.replace('?','')
                nfnb=nfnb.replace('*','!')
                nfnb=nfnb.replace('/',' ')
                nfnb=nfnb.replace('\\',' ')
                nfnb=nfnb.replace(':','-')
                nfn=f[3][1]+os.path.sep+'Season '+f[3][2]+os.path.sep+f[3][3]+', '+nfnb
                print('IdentTV:    %s'%(nfn))
                f[1]=nfn
                queue[1].remove(f)
                queue[2].append(f)
            else:
                ignore.append(f)
                queue[1].remove(f)
                print("UnhandledMeta: %s"%f[1]+f[2])
                continue
        # if len(queue[0])==0:
        #     with open('test.json','w+') as f:
        #         f.write(json.dumps(queue,indent=4))
        #     with open('tvmc.json','w+') as f:
        #         f.write(json.dumps(tvmc,indent=4))
        #     with open('mmc.json','w+') as f:
        #         f.write(json.dumps(mmc,indent=4))
        #     exit()
        # continue
        for ca in ['tvmc','mmc','tmdbdc','tmdbsc']:
            if os.path.exists(ca):
                with open(ca,'wb') as q:
                    pickle.load(queue,q)
                    q.close()


        savQueState()
        # Stage 2, MediaProbe
        for f in queue[2].copy():
            savQueState()
            print("ProbeStart: %s"%f[0])
            # mStreams=mProbe(os.path.abspath(f[0])])
            # if not mStreams:
            #     continue
            #
            ret=Popen(['ffprobe','-show_format','-show_streams','-loglevel','quiet','-print_format','json','-i',os.path.abspath(f[0])],stderr=STDOUT,stdout=PIPE)
            out=ret.communicate()[0]
            ret=ret.returncode
            if ret!=0:
                ignore.append(f)
                queue[2].remove(f)
                print("ProbeFail: %s"%f[1]+f[2])
                import code
                code.interact(local=dict(globals(), **locals()))
                continue
            mp=json.loads(out)
            # print(f[0])
            # print(json.dumps(mp["streams"],indent=2))
            # try:
            asc=0
            asl=[]
            for d in mp["streams"]:
                if d['codec_type']=="audio":
                    asc+=1
            for d in mp["streams"]:
                ass=[]
                if d['codec_type']=="audio":
                    if 'tags' in d:
                        ltag=None
                        for tag in d['tags']:
                            if tag.lower()=='language':
                                if d['tags'][tag]=="eng":
                                    ass.append(d['index'])
                                    ass.append(d['codec_name'])
                                    ass.append(d['channels'])
                                    if 'profile' in d:
                                        ass.append(d['profile'])

                    if len(ass)==0 and asc==1:
                        ass.append(d['index'])
                        ass.append(d['codec_name'])
                        ass.append(d['channels'])
                        if 'profile' in d:
                            ass.append(d['profile'])
                if len(ass)>0:
                    asl.append(ass)
            if len(asl)==0:
                for d in mp["streams"]:
                    ass=[]
                    if d['codec_type']=="audio":
                        if 'tags' in d:
                            ltag=None
                            for tag in d['tags']:
                                if tag.lower()=='language':
                                    if d['tags'][tag]=="und":
                                        ass.append(d['index'])
                                        ass.append(d['codec_name'])
                                        ass.append(d['channels'])
                                        if 'profile' in d:
                                            ass.append(d['profile'])
                    if len(ass)>0:
                        asl.append(ass)
            if len(asl)==0:
                ignore.append(f)
                queue[2].remove(f)
                print("ProbeFail: %s,NoAudio"%f[0])
                continue
            f.append(asl)
            # f.append([[d['index'],d['codec_name'],d['channels'],d['profile']] for d in mp["streams"] if (d['codec_type']=="audio" and d['tags']['language']=="eng")])
            # except:
                # try:
                    # f.append([[d['index'],d['codec_name'],d['channels']] for d in mp["streams"] if (d['codec_type']=="audio" and d['tags']['language']=="eng")])
                # except:
                #     try:
                        # f.append([[d['index'],d['codec_name'],d['channels']] for d in mp["streams"] if (d['codec_type']=="audio")])
                #         print("ProbeWarn,AudLangMissing: %s"%f[0])
                #     except:
                #         ignore.append(f)
                #         queue[2].remove(f)
                #         print("[Audio]")
                #         print(mp["streams"])
                #         print("ProbeFail: %s,NoAudio"%f[0])
            vsl=[]
            for d in mp["streams"]:
                if not d['codec_type']=="video":
                    continue
                vstr=[d['index'],d['codec_name']]
                if 'nb_frames' in d:
                    vstr.append(d['nb_frames'])
                else:
                    bGotFrames=False
                    if 'tags' in d:
                        if 'NUMBER_OF_FRAMES-eng' in d['tags']:
                            vstr.append(d['tags']['NUMBER_OF_FRAMES-eng'])
                            bGotFrames=True
                    if not bGotFrames:
                        ret=Popen(['mediainfo','--Output=Video;%FrameCount%',os.path.abspath(f[0])],stderr=STDOUT,stdout=PIPE)
                        ret=ret.communicate()[0].strip()
                        print(ret)
                        if len(ret):
                            if int(ret):
                                vstr.append(int(ret))
                            else:
                                print("ProbeFail: %s,NoFrameCount"%f[0])
                                ignore.append(f)
                                queue[2].remove(f)
                                # f.append([[d['index'],d['codec_name'],1] for d in mp["streams"] if (d['codec_type']=="video")])
                                continue
                        else:
                            print("ProbeWarn: %s,ManualFrameCount"%f[0])
                            ret=Popen(['ffprobe','-v','error','-count_frames','-select_streams','v:0','-show_entries','stream=nb_read_frames','-of','default=nokey=1:noprint_wrappers=1',os.path.abspath(f[0])],stderr=STDOUT,stdout=PIPE)
                            ret=ret.communicate()[0]
                            if ret:
                                vstr.append(int(ret))
                            else:
                                print("ProbeFail: %s,NoFrameCount"%f[0])
                                ignore.append(f)
                                queue[2].remove(f)
                                # f.append([[d['index'],d['codec_name'],1] for d in mp["streams"] if (d['codec_type']=="video")])
                                continue
                if len(vstr)!=3:
                    print(vstr)
                    import code
                    code.interact(local=dict(globals(), **locals()))
                vsl.append(vstr)
            if len(vsl)==0:
                ignore.append(f)
                queue[2].remove(f)
                print("[Video]")
                print(json.dumps(mp["streams"],indent=2))
                print("ProbeFail: %s,NoVideo"%f[0])
                continue
            f.append(vsl)
            #
            # try:
            #     f.append([[d['index'],d['codec_name'],d['tags']['NUMBER_OF_FRAMES-eng']] for d in mp["streams"] if (d['codec_type']=="video")])
            # except:
            #     try:
            #         f.append([[d['index'],d['codec_name'],d['nb_frames']] for d in mp["streams"] if (d['codec_type']=="video")])
            #     except:
            #         try:
            #             ret=Popen(['mediainfo','--Output=Video;%FrameCount%',os.path.abspath(f[0])],stderr=STDOUT,stdout=PIPE)
            #             ret=int(ret.communicate()[0])
            #             if ret:
            #                 f.append([[d['index'],d['codec_name'],ret] for d in mp["streams"] if (d['codec_type']=="video")])
            #             else:
            #                 ignore.append(f)
            #                 queue[2].remove(f)
            #                 f.append([[d['index'],d['codec_name'],1] for d in mp["streams"] if (d['codec_type']=="video")])
            #                 print("ProbeFail: %s,NoFrameCount"%f[0])
            #                 continue
            #         except:
            #             ignore.append(f)
            #             queue[2].remove(f)
            #             print("[Video]")
            #             print(json.dumps(mp["streams"],indent=2))
            #             print("ProbeFail: %s,NoVideo"%f[0])
            if f in queue[2]:
                queue[2].remove(f)
                queue[3].append(f)
                print("ProbeEnd: %s"%f[0])
                continue
        # continue
        # Stage 3, Recoding
        for i,f in enumerate(queue[3].copy()):
            savQueState()
            encoutact=False
            print("RecodeStart: [%s:%s], %s"%(i,len(queue[3]),f[0]))
            fpath=os.path.abspath('staging'+os.path.sep+f[1]+'.mp4')
            if os.path.exists(fpath):
                ignore.append(f)
                queue[3].remove(f)
                print("RecodeFail: %s,PreExists"%('staging'+os.path.sep+f[1]+'.mp4'))
                continue
            keeps=False
            cmd=['ffmpeg','-hide_banner','-fflags','+genpts','-i',os.path.abspath(f[0])]
            sv=f[len(f)-1]
            sa=f[len(f)-2]
            # print([sv,sa])
            print(f)
            # exit()
            if len(sv)>1:
                # ignore.append(f)
                print("RecodeWarn: %s,MultiVideo"%f[0])

                # keeps=True
            if len(sa)>1:
                ignore.append(f)
                print("RecodeWarn: %s,MultiEngAudio"%f[0])
                keeps=True
            svf=1
            if len(sv)>0:
                if len(sv[0])>2:
                    svf=int(sv[0][2])
            if svf<=1:
                ret=Popen(['mediainfo','--Output=Video;%FrameCount%',os.path.abspath(f[0])],stderr=STDOUT,stdout=PIPE)
                out=ret.communicate()[0]
                ret=ret.returncode
                if ret==0 and out!=b'\r\n':
                    # print(out)
                    svf=int(out)
            print(sv)
            if str(sv[0][1]) in ['msmpeg4v3']:
                cmd.extend(['-c:v','h264','-map','0:'+str(sv[0][0])])
            else:
                cmd.extend(['-c:v','copy','-map','0:'+str(sv[0][0])])
            print(sa)
            if str(sa[0][1]) in ['flac','truehd','msmpeg4v3','opus']:
                cmd.extend(['-c:a','aac','-map','0:'+str(sa[0][0])])
            else:
                cmd.extend(['-c:a','copy','-map','0:'+str(sa[0][0])])
            cmd.extend(['-map_metadata','-1','-map_chapters','-1','-sn','-n','-threads','8'])
            # cmd.extend(['-movflags','+faststart'])
            path=Path(fpath)
            path.parent.mkdir(parents=True, exist_ok=True)
            cmd.append(fpath)
            print(cmd)
            ret=Popen(cmd,stderr=STDOUT,stdout=PIPE,universal_newlines=True,bufsize=1)
            lastLine=None
            exitCode=None
            out_log=[]
            cerr_log=[]
            while True:
                exitCode=ret.poll()
                for out_line, err_line in read_popen_pipes(ret):
                    err_line=err_line.replace('\n','').strip()
                    out_line=out_line.replace('\n','').strip()
                    if err_line!='':
                        cerr_log.append(err_line)
                    if out_line!='':
                        ffProg=re.match(ffProgPat,out_line)
                        # print('['+out_line+']')
                        if ffProg:
                            ffProgl=list(ffProg.groups())
                            for e,i in enumerate(ffProgl):
                                try:
                                    a = float(e)
                                    b = int(e)
                                    if a != b:
                                        ffProgl[i]=a
                                    else:
                                        ffProgl[i]=b
                                except:
                                    pass
                            ffProgl[0]=int(ffProgl[0])
                            try:
                                ffProgl[1]=int(ffProgl[1])
                            except:
                                ffProgl[1]=float(ffProgl[1])
                            try:
                                ffProgl[6]=float(ffProgl[6])
                            except:
                                pass
                            restart_line()
                            print([round((int(ffProgl[0])/int(svf))*100,2),ffProgl],end='')
                        else:
                            out_log.append(out_line)
                            if encoutact:
                                # if 'error' in out_line:
                                if re.search(r'(?i)\berror\b',out_line):
                                    # log.append(out_line)
                                    print('[RecodeError]:'+out_line)
                                    lastLine=out_line
                                    exitCode=1
                                    keeps=True
                                    ret.kill()
                                    break
                                print(out_line)
                                print('')
                            if out_line=="Press [q] to stop, [?] for help":
                                encoutact=True
                            time.sleep(0.000001)
                            pass
                if exitCode is not None:
                    lastLine=out_line
                    break
            if exitCode!=0:
                print('')
                ignore.append(f)
                queue[3].remove(f)
                err_log[fpath]={'stderr':cerr_log,'stdout':out_log}
                if os.path.exists(fpath):
                    os.chmod(fpath,0o777)
                    os.remove(fpath)
                print("RecodeFail: %s,%s"%(f[0],lastLine))
                with open('error.log','w+') as log_f:
                    log_f.write(json.dumps(err_log,indent=2))
                continue
            # if os.path.getsize(os.path.abspath(fpath))==0:
            #     keeps=True
            if not keeps:
                os.chmod(os.path.abspath(f[0]),0o777)
                os.remove(os.path.abspath(f[0]))
                print('')
                print("RecodeDone: %s"%('staging'+os.path.sep+f[1]+'.mp4'))
            # f[0]='staging'+os.path.sep+f[1]+'.mp4'
            if f in queue[3]:
                queue[3].remove(f)
                queue[4].append(f)
        # Stage 4, Tagging
        for f in queue[4].copy():
            savQueState()
            msp='staging'+os.path.sep+f[1]+'.mp4'
            try:
                print("Tagging: %s"%msp)
                md=f[3]
                mdd=None
                if md[0]=='tv':
                    mdd=md[4]
                if md[0]=='movie':
                    mdd=f[4]
                # print(mdd)
                video=MP4(os.path.abspath(msp))
                video['\xa9nam'] = mdd[0]
                video['\xa9day'] = mdd[1]
                post=None
                if md[0]=='tv':
                    tvs=tvmc[mdd[4]]
                    # tvss=tvs[6]
                    # print(tvss[int(md[2])][0])
                    video['desc'] = mdd[3]
                    video['ldes'] = mdd[3]
                    video['\xa9gen'] = ', '.join(mdd[2])
                    cast=mdd[2]
                    cast.extend(tvs[5])
                    # print(cast)
                    video['aART'] = ', '.join(cast)
                    post=tvs[6][int(md[2])][0]
                if md[0]=='movie':
                    video['aART'] = ', '.join(mdd[7])
                    post=mdd[2]
                conn = http.client.HTTPSConnection("image.tmdb.org")
                conn.request("GET", "/t/p/w780"+post)
                res = conn.getresponse()
                video["covr"] = [MP4Cover(res.read(),imageformat=MP4Cover.FORMAT_JPEG)]
                video.save()
                print("TagDone: %s"%msp)
                queue[4].remove(f)
                queue[5].append(f)
            except Exception as e:
                ignore.append(f)
                queue[4].remove(f)
                trace = traceback.format_exc()
                print("TagFail: %s,%s"%(msp,trace))
                # code.interact(local=locals())
        # Stage 6, Finishing
        for f in queue[5].copy():
            savQueState()
            msp='staging'+os.path.sep+f[1]+'.mp4'
            mdp='out'+os.path.sep+f[1]+'.mp4'
            try:
                path=Path(mdp)
                path.parent.mkdir(parents=True, exist_ok=True)
                os.chmod(os.path.abspath(msp),0o777)
                os.replace(os.path.abspath(msp),os.path.abspath(mdp))
            except:
                print("FinishFail: %s"%msp)
            ignore.append(f)
            queue[5].remove(f)
        savQueState()
        time.sleep(1)
        # Cleanup
        for f in lsdexp('in'+os.path.sep, '.*(:?\.txt|\.nfo|\.xml|\.srt|\.bat|\.db|\.png|\.pdf|\.jpe?g)$'):
            os.chmod(os.path.abspath(f[0]),0o777)
            os.remove(os.path.abspath(f[0]))
        removeEmptyFolders('staging',False)
        removeEmptyFolders('in',False)
    except Exception as e:
        savQueState()
        trace = traceback.format_exc()
        print("ProcFail: %s"%(trace))
        import code
        code.interact(local=dict(globals(), **locals()))
        continue
